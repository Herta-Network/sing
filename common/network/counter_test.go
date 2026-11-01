package network

import (
	"bytes"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type replaceableCounterReader struct{ io.Reader }

func (c *replaceableCounterReader) ReaderReplaceable() bool { return true }
func (c *replaceableCounterReader) UpstreamReader() any     { return c.Reader }
func (c *replaceableCounterReader) UnwrapReader() (io.Reader, []CountFunc) {
	return c.Reader, []CountFunc{func(int64) {}}
}

type replaceableCounterWriter struct{ io.Writer }

func (c *replaceableCounterWriter) WriterReplaceable() bool { return true }
func (c *replaceableCounterWriter) UpstreamWriter() any     { return c.Writer }
func (c *replaceableCounterWriter) UnwrapWriter() (io.Writer, []CountFunc) {
	return c.Writer, []CountFunc{func(int64) {}}
}

type emptyPacketIO struct{}

func (emptyPacketIO) ReadPacket(*buf.Buffer) (M.Socksaddr, error)    { return M.Socksaddr{}, io.EOF }
func (emptyPacketIO) WritePacket(b *buf.Buffer, _ M.Socksaddr) error { b.Release(); return nil }

type replaceablePacketCounter struct{ emptyPacketIO }

func (c *replaceablePacketCounter) ReaderReplaceable() bool { return true }
func (c *replaceablePacketCounter) WriterReplaceable() bool { return true }
func (c *replaceablePacketCounter) UpstreamReader() any     { return c.emptyPacketIO }
func (c *replaceablePacketCounter) UpstreamWriter() any     { return c.emptyPacketIO }
func (c *replaceablePacketCounter) UnwrapPacketReader() (PacketReader, []CountFunc) {
	return c.emptyPacketIO, []CountFunc{func(int64) {}}
}
func (c *replaceablePacketCounter) UnwrapPacketWriter() (PacketWriter, []CountFunc) {
	return c.emptyPacketIO, []CountFunc{func(int64) {}}
}
func TestReplaceableCountersAreCollectedBeforeUnwrapping(t *testing.T) {
	_, read := UnwrapCountReader(&replaceableCounterReader{bytes.NewReader(nil)}, nil)
	_, write := UnwrapCountWriter(&replaceableCounterWriter{io.Discard}, nil)
	_, packetRead := UnwrapCountPacketReader(&replaceablePacketCounter{}, nil)
	_, packetWrite := UnwrapCountPacketWriter(&replaceablePacketCounter{}, nil)
	for name, counts := range map[string][]CountFunc{"read": read, "write": write, "packet read": packetRead, "packet write": packetWrite} {
		if len(counts) != 1 {
			t.Errorf("%s: got %d counters", name, len(counts))
		}
	}
}
