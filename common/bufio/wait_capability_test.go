package bufio

import (
	"bytes"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type capableReader struct{ io.Reader }

func (c *capableReader) ReaderReplaceable() bool                     { return true }
func (c *capableReader) UpstreamReader() any                         { return c.Reader }
func (c *capableReader) InitializeReadWaiter(N.ReadWaitOptions) bool { return false }
func (c *capableReader) WaitReadBuffer() (*buf.Buffer, error)        { return nil, io.EOF }
func (c *capableReader) WaitReadBuffers() ([]*buf.Buffer, error)     { return nil, io.EOF }

type capablePacketReader struct{}

func (c *capablePacketReader) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, io.EOF
}
func (c *capablePacketReader) ReaderReplaceable() bool                     { return true }
func (c *capablePacketReader) UpstreamReader() any                         { return &fallbackTestPacketReader{} }
func (c *capablePacketReader) InitializeReadWaiter(N.ReadWaitOptions) bool { return false }
func (c *capablePacketReader) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return nil, M.Socksaddr{}, io.EOF
}

type fallbackTestPacketReader struct{}

func (c *fallbackTestPacketReader) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, io.EOF
}
func TestWaiterCapabilitiesSurviveReplaceableWrappers(t *testing.T) {
	reader := &capableReader{bytes.NewReader(nil)}
	if waiter, ok := CreateReadWaiter(reader); !ok || waiter != reader {
		t.Fatal("scalar waiter bypassed")
	}
	if waiter, ok := CreateVectorisedReadWaiter(reader); !ok || waiter != reader {
		t.Fatal("vector waiter bypassed")
	}
	packet := &capablePacketReader{}
	if waiter, ok := CreatePacketReadWaiter(packet); !ok || waiter != packet {
		t.Fatal("packet waiter bypassed")
	}
	if unwrapped, _ := N.UnwrapCountReader(reader, nil); unwrapped != reader {
		t.Fatal("counter unwrapping bypassed waiter")
	}
}
