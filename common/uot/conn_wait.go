package uot

import (
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func (c *Conn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	c.readWaitOptions = options
	return false
}

func (c *Conn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
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
	buffer = c.readWaitOptions.NewBufferSize(length)
	_, err = buffer.ReadFullFrom(c.Conn, length)
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, E.Cause(err, "UoT read")
	}
	c.readWaitOptions.PostReturn(buffer)
	return
}
