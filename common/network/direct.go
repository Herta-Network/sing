package network

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type ReadWaitable interface {
	InitializeReadWaiter(options ReadWaitOptions) (needCopy bool)
}

type ReadWaitOptions struct {
	FrontHeadroom int
	RearHeadroom  int
	MTU           int
	// MaxBufferSize applies to new stream buffers, not packet buffers. Required
	// headroom plus one payload byte takes precedence over a smaller limit.
	MaxBufferSize int
	BatchSize     int
}

func NewReadWaitOptions(source any, destination any) ReadWaitOptions {
	return ReadWaitOptions{
		FrontHeadroom: CalculateFrontHeadroom(destination),
		RearHeadroom:  CalculateRearHeadroom(destination),
		MTU:           CalculateMTU(source, destination),
	}
}

func (o ReadWaitOptions) NeedHeadroom() bool {
	return o.FrontHeadroom > 0 || o.RearHeadroom > 0
}

func (o ReadWaitOptions) Copy(buffer *buf.Buffer) *buf.Buffer {
	if o.FrontHeadroom > buffer.Start() ||
		o.RearHeadroom > buffer.FreeLen() {
		newBuffer := buf.NewSize(buffer.Len() + o.FrontHeadroom + o.RearHeadroom)
		newBuffer.Resize(o.FrontHeadroom, 0)
		newBuffer.Write(buffer.Bytes())
		buffer.Release()
		return newBuffer
	} else {
		return buffer
	}
}

func (o ReadWaitOptions) NewBuffer() *buf.Buffer {
	if o.MaxBufferSize > 0 {
		size := buf.BufferSize
		if o.MTU > 0 {
			size = o.MTU + o.FrontHeadroom + o.RearHeadroom
		}
		if size > o.MaxBufferSize {
			size = o.MaxBufferSize
		}
		minimum := o.FrontHeadroom + o.RearHeadroom + 1
		if size < minimum {
			size = minimum
		}
		buffer := buf.NewSize(size)
		buffer.Resize(o.FrontHeadroom, 0)
		buffer.Reserve(o.RearHeadroom)
		return buffer
	}
	return o.newBuffer(buf.BufferSize, true)
}

func (o ReadWaitOptions) NewPacketBuffer() *buf.Buffer {
	return o.newBuffer(buf.UDPBufferSize, true)
}

func (o ReadWaitOptions) newBuffer(defaultBufferSize int, reserve bool) *buf.Buffer {
	var bufferSize int
	if o.MTU > 0 {
		bufferSize = o.MTU + o.FrontHeadroom + o.RearHeadroom
	} else {
		bufferSize = defaultBufferSize
	}
	buffer := buf.NewSize(bufferSize)
	if o.FrontHeadroom > 0 {
		buffer.Resize(o.FrontHeadroom, 0)
	}
	if o.RearHeadroom > 0 && reserve {
		buffer.Reserve(o.RearHeadroom)
	}
	return buffer
}

func (o ReadWaitOptions) PostReturn(buffer *buf.Buffer) {
	if o.RearHeadroom > 0 {
		buffer.OverCap(o.RearHeadroom)
	}
}

type ReadWaiter interface {
	ReadWaitable
	WaitReadBuffer() (buffer *buf.Buffer, err error)
}

type ReadWaitCreator interface {
	CreateReadWaiter() (ReadWaiter, bool)
}

type VectorisedReadWaiter interface {
	ReadWaitable
	// The result slice is valid until the next read, initialization or release.
	// Buffer ownership transfers to the caller. Errors return no owned buffers.
	WaitReadBuffers() ([]*buf.Buffer, error)
}

type VectorisedReadWaitCreator interface {
	CreateVectorisedReadWaiter() (VectorisedReadWaiter, bool)
}

// ReadWaiterReleaser releases waiter-owned buffers without closing the source.
// Calls must be serialized with reads and initialization. Release is idempotent
// and never releases buffers already transferred to the caller.
type ReadWaiterReleaser interface {
	ReleaseReadWaiter()
}

type PacketReadWaiter interface {
	ReadWaitable
	WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error)
}

type PacketReadWaitCreator interface {
	CreateReadWaiter() (PacketReadWaiter, bool)
}
