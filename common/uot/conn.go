package uot

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var (
	_ N.NetPacketConn    = (*Conn)(nil)
	_ N.PacketReadWaiter = (*Conn)(nil)
)

type Conn struct {
	net.Conn
	isConnect       bool
	destination     M.Socksaddr
	writer          N.VectorisedWriter
	countedWriter   N.VectorisedWriterWithCount
	readWaitOptions N.ReadWaitOptions
	readBuffer      [16]byte
	readAccess      sync.Mutex
	writeErr        error
	writeAccess     sync.Mutex
	writeVectors    [2]*buf.Buffer
	batchVectors    []*buf.Buffer
}

func NewConn(conn net.Conn, request Request) *Conn {
	uConn := &Conn{
		Conn:        conn,
		isConnect:   request.IsConnect,
		destination: request.Destination,
	}
	uConn.writer, _ = bufio.CreateVectorisedWriter(N.UnwrapWriter(conn))
	uConn.countedWriter = bufio.NewVectorisedWriterWithCount(conn)
	return uConn
}

func (c *Conn) Read(p []byte) (n int, err error) {
	n, _, err = c.ReadFrom(p)
	return
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.WriteTo(p, c.destination)
}

func (c *Conn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	var destination M.Socksaddr
	if c.isConnect {
		destination = c.destination
	} else {
		destination, err = AddrParser.ReadAddrPortWithScratch(c.Conn, c.readBuffer[:])
		if err != nil {
			return
		}
	}
	var length int
	length, err = c.readLength()
	if err != nil {
		return
	}
	if len(p) < length {
		err = c.discardPacket(length)
		return
	}
	n, err = io.ReadFull(c.Conn, p[:length])
	if err == nil {
		addr = destination.UDPAddr()
	}
	return
}

func (c *Conn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if len(p) > int(^uint16(0)) {
		return 0, E.New("UoT payload too large: ", len(p))
	}

	destination := M.SocksaddrFromNet(addr)
	var bufferLen int
	if !c.isConnect {
		bufferLen += AddrParser.AddrPortLen(destination)
	}
	bufferLen += 2
	if c.writer == nil {
		bufferLen += len(p)
	}
	buffer := buf.NewSize(bufferLen)
	ownedHeader := true
	defer func() {
		if ownedHeader {
			buffer.Release()
		}
	}()
	if !c.isConnect {
		err = AddrParser.WriteAddrPortBuffer(buffer, destination)
		if err != nil {
			return
		}
	}
	common.Must(writeLength(buffer, len(p)))
	if c.writer == nil {
		common.Must1(buffer.Write(p))
		written, writeErr := c.Conn.Write(buffer.Bytes())
		if writeErr == nil && written != buffer.Len() {
			writeErr = io.ErrShortWrite
		}
		n = written - (buffer.Len() - len(p))
		if n < 0 {
			n = 0
		}
		if n > len(p) {
			n = len(p)
		}
		return n, c.finishWrite(writeErr)
	}
	c.writeVectors[0] = buffer
	c.writeVectors[1] = buf.As(p)
	ownedHeader = false
	err = c.writer.WriteVectorised(c.writeVectors[:])
	clearBuffers(c.writeVectors[:])
	if err != nil {
		buffer.Leak()
	}
	err = c.finishWrite(err)
	if err == nil {
		n = len(p)
	}
	return
}

func (c *Conn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	if c.isConnect {
		destination = c.destination
	} else {
		destination, err = AddrParser.ReadAddrPortWithScratch(c.Conn, c.readBuffer[:])
		if err != nil {
			return
		}
	}
	var length int
	length, err = c.readLength()
	if err != nil {
		return
	}
	if buffer.FreeLen() < length {
		return M.Socksaddr{}, c.discardPacket(length)
	}
	_, err = buffer.ReadFullFrom(c.Conn, length)
	if err != nil {
		return M.Socksaddr{}, E.Cause(err, "UoT read")
	}
	return
}

// FrontHeadroom lets owned-buffer relays reserve space before reading payloads.
func (c *Conn) FrontHeadroom() int {
	if c.isConnect {
		return 2
	}
	return 2 + 1 + 1 + 255 + 2 // length, family, domain length, domain, port
}

func (c *Conn) preparePacket(buffer *buf.Buffer, destination M.Socksaddr) (*buf.Buffer, error) {
	if buffer == nil {
		return nil, os.ErrInvalid
	}
	length := buffer.Len()
	if length > int(^uint16(0)) {
		buffer.Release()
		return nil, E.New("UoT payload too large: ", length)
	}
	headerLen := 2
	if !c.isConnect {
		headerLen += AddrParser.AddrPortLen(destination)
	}
	buffer = (N.ReadWaitOptions{FrontHeadroom: headerLen}).Copy(buffer)
	header := buf.With(buffer.ExtendHeader(headerLen))
	if err := c.writeHeader(header, length, destination); err != nil {
		buffer.Release()
		return nil, err
	}
	return buffer, nil
}

func (c *Conn) writeHeader(header *buf.Buffer, length int, destination M.Socksaddr) error {
	if !c.isConnect {
		if err := AddrParser.WriteAddrPortBuffer(header, destination); err != nil {
			return err
		}
	}
	binary.BigEndian.PutUint16(header.Extend(2), uint16(length))
	return nil
}

func (c *Conn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	if c.writeErr != nil {
		buffer.Release()
		return c.writeErr
	}
	frame, err := c.preparePacket(buffer, destination)
	if err != nil {
		return err
	}
	total := int64(frame.Len())
	c.writeVectors[0] = frame
	n, err := c.countedWriter.WriteVectorisedCount(c.writeVectors[:1])
	c.writeVectors[0] = nil
	if n < 0 || n > total || (n != total && err == nil) {
		err = io.ErrShortWrite
	}
	return c.finishWrite(err)
}

func (c *Conn) CreatePacketBatchWriter() (N.PacketBatchWriter, bool) {
	return c, true
}

func (c *Conn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	return c, true
}

func (c *Conn) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	_, err := c.WritePacketBatchCount(buffers, destinations)
	return err
}

func (c *Conn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (written int, err error) {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	if c.writeErr != nil {
		buf.ReleaseMulti(buffers)
		return 0, c.writeErr
	}
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	for start := 0; start < len(buffers); {
		end := start + bufio.MaxReadBatchSize
		if end > len(buffers) {
			end = len(buffers)
		}
		n, writeErr := c.writePacketBatchCountLocked(buffers[start:end], destinations[start:end])
		written += n
		if writeErr != nil {
			buf.ReleaseMulti(buffers[end:])
			return written, writeErr
		}
		start = end
	}
	return written, nil
}

func (c *Conn) writePacketBatchCountLocked(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	c.batchVectors = c.batchVectors[:0]
	if cap(c.batchVectors) < 2*len(buffers) {
		c.batchVectors = make([]*buf.Buffer, 0, 2*len(buffers))
	}
	var inlineEnds [bufio.MaxReadBatchSize]int64
	frameEnds := inlineEnds[:len(buffers)]
	var total int64
	owned := true
	defer func() {
		if owned {
			buf.ReleaseMulti(c.batchVectors)
		}
		clearBuffers(c.batchVectors)
		c.batchVectors = c.batchVectors[:0]
	}()
	for index, buffer := range buffers {
		headerLen := 2
		if !c.isConnect {
			headerLen += AddrParser.AddrPortLen(destinations[index])
		}
		// Without reserved headroom, keep header and payload separate. Joining
		// an opaque transport's vectors then copies each payload only once.
		if buffer != nil && buffer.Len() <= 65535 && buffer.Start() < headerLen {
			header := buf.NewSize(headerLen)
			if err := c.writeHeader(header, buffer.Len(), destinations[index]); err != nil {
				header.Release()
				buf.ReleaseMulti(buffers[index:])
				return 0, err
			}
			c.batchVectors = append(c.batchVectors, header, buffer)
			total += int64(header.Len() + buffer.Len())
		} else {
			frame, err := c.preparePacket(buffer, destinations[index])
			if err != nil {
				buf.ReleaseMulti(buffers[index+1:])
				return 0, err
			}
			c.batchVectors = append(c.batchVectors, frame)
			total += int64(frame.Len())
		}
		frameEnds[index] = total
	}
	owned = false
	n, err := c.countedWriter.WriteVectorisedCount(c.batchVectors)
	if n < 0 || n > total {
		n, err = 0, io.ErrShortWrite
	} else if n != total && err == nil {
		err = io.ErrShortWrite
	}
	complete := 0
	for _, end := range frameEnds {
		if end > n {
			break
		}
		complete++
	}
	return complete, c.finishWrite(err)
}

func clearBuffers(buffers []*buf.Buffer) {
	for index := range buffers {
		buffers[index] = nil
	}
}

// A failed stream write may have published a partial frame. Retrying another
// datagram would desynchronize the peer; closing also wakes blocked readers.
func (c *Conn) finishWrite(err error) error {
	if err != nil {
		c.writeErr = err
		c.Conn.Close()
	}
	return err
}

func (c *Conn) readLength() (int, error) {
	_, err := io.ReadFull(c.Conn, c.readBuffer[:2])
	if err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint16(c.readBuffer[:2])), nil
}

func writeLength(buffer *buf.Buffer, length int) error {
	if length > int(^uint16(0)) {
		return E.New("UoT payload too large: ", length)
	}
	if buffer.FreeLen() < 2 {
		return io.ErrShortBuffer
	}
	binary.BigEndian.PutUint16(buffer.Extend(2), uint16(length))
	return nil
}

func (c *Conn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *Conn) Upstream() any {
	return c.Conn
}

var _ N.PacketBatchWriteCreator = (*Conn)(nil)

func (c *Conn) discardPacket(length int) error {
	if _, err := io.CopyN(io.Discard, c.Conn, int64(length)); err != nil {
		return err
	}
	return E.Cause(io.ErrShortBuffer, "UoT read")
}
