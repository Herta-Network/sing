package bufio

import (
	"io"
	"net"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// CreateReadWaiter preserves ownership of the handshake-prefetched packet while
// forwarding the underlying owned reader. It never skips the cache owner.
func (c *CachedPacketConn) CreateReadWaiter() (N.PacketReadWaiter, bool) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	if c.closed.Load() {
		return nil, false
	}
	if c.waiter == nil {
		waiter, ok := CreatePacketReadWaiter(c.PacketConn)
		if !ok {
			return nil, false
		}
		c.waiter = waiter
	}
	return &c.waiterView, true
}

type cachedPacketReadWaiter struct{ conn *CachedPacketConn }

func (r *cachedPacketReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c := r.conn
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	c.waitOptions = options
	if c.waiter != nil && !c.closed.Load() {
		c.needCopy = c.waiter.InitializeReadWaiter(options)
		c.waiterInitialized = true
	}
	// This wrapper prepares both its cached packet and the underlying output.
	return false
}

func (r *cachedPacketReadWaiter) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	c := r.conn
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	return c.waitReadPacket()
}

func (c *CachedPacketConn) waitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	if c.closed.Load() || !c.waiterInitialized {
		return nil, M.Socksaddr{}, net.ErrClosed
	}
	if c.buffer != nil {
		buffer := c.buffer
		c.buffer = nil
		buffer.DecRef()
		return c.waitOptions.Copy(buffer), c.destination, nil
	}
	buffer, destination, err := c.waiter.WaitReadPacket()
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	if buffer == nil {
		return nil, M.Socksaddr{}, io.ErrNoProgress
	}
	if c.needCopy {
		buffer = c.waitOptions.Copy(buffer)
	}
	return buffer, destination, nil
}

func (r *cachedPacketReadWaiter) ReleaseReadWaiter() {
	c := r.conn
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	c.releaseWaiter()
}

func (c *CachedPacketConn) releaseWaiter() {
	if releaser, ok := c.waiter.(N.ReadWaiterReleaser); ok {
		releaser.ReleaseReadWaiter()
	}
	c.waiter = nil
	c.waiterInitialized = false
}
