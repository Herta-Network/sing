package udpnat

import (
	"io"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var _ N.PacketReadWaiter = (*conn)(nil)

func (c *conn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.ReleaseReadWaiter()
	c.readWaitOptions = options
	return false
}

func (c *conn) CreatePacketBatchReadWaiter() (N.PacketBatchReadWaiter, bool) { return c, true }

func (c *conn) WaitReadPackets() ([]*buf.Buffer, []M.Socksaddr, error) {
	buffer, destination, err := c.WaitReadPacket()
	if err != nil {
		return nil, nil, err
	}
	batchSize := c.readWaitOptions.BatchSize
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
	for len(c.batchBuffers) < batchSize && !common.Done(c.ctx) {
		select {
		case packet := <-c.data:
			if common.Done(c.ctx) {
				releasePacket(packet)
				return c.batchBuffers, c.batchAddresses, nil
			}
			c.batchBuffers = append(c.batchBuffers, c.readWaitOptions.Copy(packet.Buffer))
			c.batchAddresses = append(c.batchAddresses, packet.Destination)
			N.PutPacketBuffer(packet)
		default:
			return c.batchBuffers, c.batchAddresses, nil
		}
	}
	return c.batchBuffers, c.batchAddresses, nil
}

// Scratch results are borrowed; never release payloads already handed out.
func (c *conn) ReleaseReadWaiter() { c.batchBuffers, c.batchAddresses = nil, nil }

func (c *conn) CreatePacketBatchWriter() (N.PacketBatchWriter, bool) {
	_, ok := c.packetBatchWriter()
	if !ok {
		return nil, false
	}
	return c, true
}

func (c *conn) packetBatchWriter() (N.PacketBatchWriter, bool) {
	c.stateAccess.RLock()
	closed, source, writer := c.closed || common.Done(c.ctx), c.source, c.batchWriter
	c.stateAccess.RUnlock()
	if closed || source == nil {
		return nil, false
	}
	if writer != nil {
		return writer, true
	}
	created := bufio.NewPacketBatchWriter(source)
	c.stateAccess.Lock()
	defer c.stateAccess.Unlock()
	if c.closed || common.Done(c.ctx) {
		return nil, false
	}
	if c.batchWriter == nil {
		c.batchWriter = created
	}
	return c.batchWriter, true
}

func (c *conn) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	writer, ok := c.packetBatchWriter()
	if !ok {
		buf.ReleaseMulti(buffers)
		return io.ErrClosedPipe
	}
	return writer.WritePacketBatch(buffers, destinations)
}

func (c *conn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	p, err := c.readPacket()
	if err != nil {
		return nil, M.Socksaddr{}, err
	}
	buffer = c.readWaitOptions.Copy(p.Buffer)
	destination = p.Destination
	N.PutPacketBuffer(p)
	return buffer, destination, nil
}
