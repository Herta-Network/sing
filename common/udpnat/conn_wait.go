package udpnat

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var _ N.PacketReadWaiter = (*conn)(nil)

func (c *conn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readWaitOptions = options
	return false
}

func (c *conn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	p, err := c.readPacket()
	if err != nil {
		return nil, M.Socksaddr{}, err
	}
	return c.readWaitOptions.Copy(p.data), p.destination, nil
}
