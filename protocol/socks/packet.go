package socks

import (
	"bytes"
	"io"
	"net"
	"sync"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// +----+------+------+----------+----------+----------+
// |RSV | FRAG | ATYP | DST.ADDR | DST.PORT |   DATA   |
// +----+------+------+----------+----------+----------+
// | 2  |  1   |  1   | Variable |    2     | Variable |
// +----+------+------+----------+----------+----------+

var ErrInvalidPacket = E.New("socks5: invalid packet")

type AssociatePacketConn struct {
	N.AbstractConn
	conn         N.ExtendedConn
	remoteAddr   M.Socksaddr
	remoteAccess sync.RWMutex
	underlying   net.Conn
}

func NewAssociatePacketConn(conn net.Conn, remoteAddr M.Socksaddr, underlying net.Conn) *AssociatePacketConn {
	return &AssociatePacketConn{
		AbstractConn: conn,
		conn:         bufio.NewExtendedConn(conn),
		remoteAddr:   remoteAddr,
		underlying:   underlying,
	}
}

func (c *AssociatePacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = c.conn.Read(p)
	if err != nil {
		return
	}
	if n < 3 {
		return 0, nil, ErrInvalidPacket
	}
	reader := bytes.NewReader(p[3:n])
	destination, err := M.SocksaddrSerializer.ReadAddrPort(reader)
	if err != nil {
		return
	}
	c.setRemoteAddr(destination)
	addr = destination.UDPAddr()
	index := 3 + int(reader.Size()) - reader.Len()
	n = copy(p, p[index:n])
	return
}

func (c *AssociatePacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	destination := M.SocksaddrFromNet(addr)
	buffer := buf.NewSize(3 + M.SocksaddrSerializer.AddrPortLen(destination) + len(p))
	defer buffer.Release()
	common.Must(buffer.WriteZeroN(3))
	err = M.SocksaddrSerializer.WriteAddrPortBuffer(buffer, destination)
	if err != nil {
		return
	}
	_, err = buffer.Write(p)
	if err != nil {
		return
	}
	written, err := c.conn.Write(buffer.Bytes())
	if err == nil && written != buffer.Len() {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *AssociatePacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	err = c.conn.ReadBuffer(buffer)
	if err != nil {
		return
	}
	if buffer.Len() < 3 {
		return M.Socksaddr{}, ErrInvalidPacket
	}
	buffer.Advance(3)
	destination, err = M.SocksaddrSerializer.ReadAddrPortBuffer(buffer)
	if err != nil {
		return
	}
	c.setRemoteAddr(destination)
	return destination.Unwrap(), nil
}

func (c *AssociatePacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	header := buf.With(buffer.ExtendHeader(3 + M.SocksaddrSerializer.AddrPortLen(destination)))
	common.Must(header.WriteZeroN(3))
	err := M.SocksaddrSerializer.WriteAddrPortBuffer(header, destination)
	if err != nil {
		buffer.Release()
		return err
	}
	return c.conn.WriteBuffer(buffer)
}

func (c *AssociatePacketConn) Read(b []byte) (n int, err error) {
	n, _, err = c.ReadFrom(b)
	return
}

func (c *AssociatePacketConn) Write(b []byte) (n int, err error) {
	return c.WriteTo(b, c.getRemoteAddr())
}

func (c *AssociatePacketConn) RemoteAddr() net.Addr {
	return c.getRemoteAddr().UDPAddr()
}

func (c *AssociatePacketConn) Upstream() any {
	return c.conn
}

func (c *AssociatePacketConn) FrontHeadroom() int {
	return 3 + M.MaxSocksaddrLength
}

func (c *AssociatePacketConn) Close() error {
	return common.Close(
		c.conn,
		c.underlying,
	)
}

func (c *AssociatePacketConn) setRemoteAddr(addr M.Socksaddr) {
	c.remoteAccess.Lock()
	c.remoteAddr = addr
	c.remoteAccess.Unlock()
}
func (c *AssociatePacketConn) getRemoteAddr() M.Socksaddr {
	c.remoteAccess.RLock()
	defer c.remoteAccess.RUnlock()
	return c.remoteAddr
}
