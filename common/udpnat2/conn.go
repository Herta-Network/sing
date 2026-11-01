package udpnat

import (
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/canceler"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/pipe"
	"github.com/metacubex/sing/contrab/freelru"
)

type Conn interface {
	N.PacketConn
	SetHandler(handler N.UDPHandlerEx)
	canceler.PacketConn
}

var _ Conn = (*natConn)(nil)
var _ N.PacketBatchReadWaitCreator = (*natConn)(nil)
var _ N.PacketBatchWriteCreator = (*natConn)(nil)

type natConn struct {
	cache            freelru.Cache[netip.AddrPort, *natConn]
	writer           N.PacketWriter
	batchWriter      N.PacketBatchWriter
	batchCountWriter N.PacketBatchWriterWithCount
	localAddr        M.Socksaddr
	handlerAccess    sync.RWMutex
	handler          N.UDPHandlerEx
	closed           bool
	closeError       error
	onClose          N.CloseHandlerFunc
	packetChan       chan *N.PacketBuffer
	doneChan         chan struct{}
	readDeadline     pipe.Deadline
	readWaitOptions  N.ReadWaitOptions
	// One reader owns these borrowed result slices, including reinitialization.
	batchBuffers   []*buf.Buffer
	batchAddresses []M.Socksaddr
}

func (c *natConn) isClosed() bool {
	select {
	case <-c.doneChan:
		return true
	default:
		return false
	}
}
func (c *natConn) readOptions() N.ReadWaitOptions {
	c.handlerAccess.RLock()
	defer c.handlerAccess.RUnlock()
	return c.readWaitOptions
}
func (c *natConn) initialize(writer N.PacketWriter, onClose N.CloseHandlerFunc) bool {
	c.handlerAccess.Lock()
	if c.closed {
		err := c.closeError
		c.handlerAccess.Unlock()
		if onClose != nil {
			onClose(err)
		}
		return false
	}
	c.writer, c.onClose = writer, onClose
	c.handlerAccess.Unlock()
	return true
}

func releasePacket(packet *N.PacketBuffer) { packet.Buffer.Release(); N.PutPacketBuffer(packet) }
func (c *natConn) deliver(buffer *buf.Buffer, destination M.Socksaddr) {
	c.handlerAccess.RLock()
	if c.closed {
		c.handlerAccess.RUnlock()
		buffer.Release()
		return
	}
	handler, options := c.handler, c.readWaitOptions
	if handler != nil {
		c.handlerAccess.RUnlock()
		handler.NewPacketEx(options.Copy(buffer), destination)
		return
	}
	packet := N.NewPacketBuffer()
	*packet = N.PacketBuffer{Buffer: buffer, Destination: destination}
	select {
	case c.packetChan <- packet:
	default:
		releasePacket(packet)
	}
	c.handlerAccess.RUnlock()
}

func (c *natConn) readPacket() (*N.PacketBuffer, error) {
	if c.isClosed() {
		return nil, io.ErrClosedPipe
	}
	select {
	case packet := <-c.packetChan:
		if c.isClosed() {
			releasePacket(packet)
			return nil, io.ErrClosedPipe
		}
		return packet, nil
	case <-c.doneChan:
		return nil, io.ErrClosedPipe
	case <-c.readDeadline.Wait():
		return nil, os.ErrDeadlineExceeded
	}
}
func (c *natConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	packet, err := c.readPacket()
	if err != nil {
		return M.Socksaddr{}, err
	}
	defer releasePacket(packet)
	if buffer.FreeLen() < packet.Buffer.Len() {
		return packet.Destination, io.ErrShortBuffer
	}
	_, err = buffer.Write(packet.Buffer.Bytes())
	return packet.Destination, err
}
func (c *natConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.handlerAccess.RLock()
	writer, closed := c.writer, c.closed
	c.handlerAccess.RUnlock()
	if closed || writer == nil {
		buffer.Release()
		return io.ErrClosedPipe
	}
	return writer.WritePacket(buffer, destination)
}
func (c *natConn) CreatePacketBatchWriter() (N.PacketBatchWriter, bool) { return c, true }
func (c *natConn) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	c.handlerAccess.Lock()
	if c.closed || c.writer == nil {
		c.handlerAccess.Unlock()
		buf.ReleaseMulti(buffers)
		return io.ErrClosedPipe
	}
	if c.batchWriter == nil {
		c.batchWriter = bufio.NewPacketBatchWriter(c.writer)
	}
	writer := c.batchWriter
	c.handlerAccess.Unlock()
	return writer.WritePacketBatch(buffers, destinations)
}
func (c *natConn) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c.ReleaseReadWaiter()
	c.handlerAccess.Lock()
	c.readWaitOptions = options
	c.handlerAccess.Unlock()
	return false
}
func (c *natConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	packet, err := c.readPacket()
	if err != nil {
		return nil, M.Socksaddr{}, err
	}
	buffer, addr := c.readOptions().Copy(packet.Buffer), packet.Destination
	N.PutPacketBuffer(packet)
	return buffer, addr, nil
}
func (c *natConn) CreatePacketBatchReadWaiter() (N.PacketBatchReadWaiter, bool) { return c, true }
func (c *natConn) WaitReadPackets() ([]*buf.Buffer, []M.Socksaddr, error) {
	buffer, destination, err := c.WaitReadPacket()
	if err != nil {
		return nil, nil, err
	}
	options := c.readOptions()
	batchSize := options.BatchSize
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > bufio.MaxReadBatchSize {
		batchSize = bufio.MaxReadBatchSize
	}
	for index := range c.batchBuffers {
		c.batchBuffers[index] = nil
	}
	for index := range c.batchAddresses {
		c.batchAddresses[index] = M.Socksaddr{}
	}
	c.batchBuffers, c.batchAddresses = c.batchBuffers[:0], c.batchAddresses[:0]
	if cap(c.batchBuffers) < batchSize {
		c.batchBuffers = make([]*buf.Buffer, 0, batchSize)
		c.batchAddresses = make([]M.Socksaddr, 0, batchSize)
	}
	c.batchBuffers = append(c.batchBuffers, buffer)
	c.batchAddresses = append(c.batchAddresses, destination)
	for len(c.batchBuffers) < batchSize && !c.isClosed() {
		select {
		case packet := <-c.packetChan:
			if c.isClosed() {
				releasePacket(packet)
				return c.batchBuffers, c.batchAddresses, nil
			}
			c.batchBuffers = append(c.batchBuffers, options.Copy(packet.Buffer))
			c.batchAddresses = append(c.batchAddresses, packet.Destination)
			N.PutPacketBuffer(packet)
		default:
			return c.batchBuffers, c.batchAddresses, nil
		}
	}
	return c.batchBuffers, c.batchAddresses, nil
}
func (c *natConn) ReleaseReadWaiter() { c.batchBuffers, c.batchAddresses = nil, nil }

// SetHandler transfers queued packets to handler. Callback invocations may
// overlap with packets already in flight; callbacks must support concurrent
// delivery and Close. No callback runs while the connection lock is held.
func (c *natConn) SetHandler(handler N.UDPHandlerEx) {
	c.handlerAccess.Lock()
	if c.closed {
		c.handlerAccess.Unlock()
		common.Close(handler)
		return
	}
	c.handler = handler
	c.readWaitOptions = N.NewReadWaitOptions(c.writer, handler)
	options := c.readWaitOptions
	c.handlerAccess.Unlock()
	if handler == nil {
		return
	}
	for !c.isClosed() {
		select {
		case packet := <-c.packetChan:
			if c.isClosed() {
				releasePacket(packet)
				return
			}
			buffer, addr := options.Copy(packet.Buffer), packet.Destination
			N.PutPacketBuffer(packet)
			handler.NewPacketEx(buffer, addr)
		default:
			return
		}
	}
}
func (c *natConn) Timeout() time.Duration {
	value, deadline, ok := c.cache.PeekWithLifetime(c.localAddr.AddrPort())
	if !ok || value != c {
		return 0
	}
	return time.Until(deadline)
}
func (c *natConn) SetTimeout(timeout time.Duration) bool {
	if c.isClosed() {
		return false
	}
	return c.cache.UpdateLifetime(c.localAddr.AddrPort(), c, timeout)
}
func (c *natConn) closeWithError(err error) {
	c.handlerAccess.Lock()
	if c.closed {
		c.handlerAccess.Unlock()
		return
	}
	c.closed = true
	c.closeError = err
	close(c.doneChan)
	handler, onClose := c.handler, c.onClose
	c.handler, c.onClose = nil, nil
	for {
		select {
		case packet := <-c.packetChan:
			releasePacket(packet)
		default:
			c.handlerAccess.Unlock()
			common.Close(handler)
			if onClose != nil {
				onClose(err)
			}
			return
		}
	}
}
func (c *natConn) Close() error                      { c.closeWithError(net.ErrClosed); return nil }
func (c *natConn) LocalAddr() net.Addr               { return c.localAddr }
func (c *natConn) RemoteAddr() net.Addr              { return M.Socksaddr{} }
func (c *natConn) SetDeadline(time.Time) error       { return os.ErrInvalid }
func (c *natConn) SetReadDeadline(t time.Time) error { c.readDeadline.Set(t); return nil }
func (c *natConn) SetWriteDeadline(time.Time) error  { return os.ErrInvalid }
func (c *natConn) Upstream() any {
	c.handlerAccess.RLock()
	defer c.handlerAccess.RUnlock()
	return c.writer
}

func (c *natConn) CreatePacketBatchWriterWithCount() (N.PacketBatchWriterWithCount, bool) {
	return c, true
}
func (c *natConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	c.handlerAccess.Lock()
	if c.closed || c.writer == nil {
		c.handlerAccess.Unlock()
		buf.ReleaseMulti(buffers)
		return 0, io.ErrClosedPipe
	}
	if c.batchCountWriter == nil {
		c.batchCountWriter = bufio.NewPacketBatchWriterWithCount(c.writer)
	}
	writer := c.batchCountWriter
	c.handlerAccess.Unlock()
	return writer.WritePacketBatchCount(buffers, destinations)
}
