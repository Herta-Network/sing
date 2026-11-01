package uot

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type ownedFrameStream struct {
	memoryPacketStream
	expected *buf.Buffer
	inPlace  bool
	vectors  int
}

func (s *ownedFrameStream) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(buffers)
	s.vectors = len(buffers)
	s.inPlace = len(buffers) == 1 && buffers[0] == s.expected
	var n int64
	for _, b := range buffers {
		written, _ := s.output.Write(b.Bytes())
		n += int64(written)
	}
	return n, nil
}

func TestOwnedPacketUsesReservedHeadroom(t *testing.T) {
	for _, connect := range []bool{true, false} {
		for _, target := range []M.Socksaddr{
			M.ParseSocksaddr("1.2.3.4:53"), M.ParseSocksaddr("[2001:db8::1]:443"),
			{Fqdn: strings.Repeat("a.", 126) + "a", Port: 443},
		} {
			for _, payload := range [][]byte{nil, []byte("payload"), bytes.Repeat([]byte{0x5a}, 65535)} {
				stream := &ownedFrameStream{}
				c := NewConn(stream, Request{IsConnect: connect, Destination: target})
				options := N.NewReadWaitOptions(nil, c)
				packet := options.NewBufferSize(len(payload))
				packet.Write(payload)
				stream.expected = packet
				if err := c.WritePacket(packet, target); err != nil {
					t.Fatal(err)
				}
				if !stream.inPlace || stream.vectors != 1 || packet.RawCap() != 0 {
					t.Fatal("owned frame copied or retained")
				}
				reader := NewConn(&memoryPacketStream{input: bytes.NewReader(stream.output.Bytes())}, Request{IsConnect: connect, Destination: target})
				got, destination, err := reader.WaitReadPacket()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got.Bytes(), payload) || destination != target {
					t.Fatal("wire frame changed")
				}
				got.Release()
			}
		}
	}
}

func TestOwnedScalarPartialWriteConsumesAndPoisons(t *testing.T) {
	stream := &partialFrameStream{limit: 3}
	c := NewConn(stream, Request{IsConnect: true})
	packet := buf.NewSize(6)
	packet.Resize(2, 4)
	if err := c.WritePacket(packet, M.Socksaddr{}); err != io.ErrShortWrite || packet.RawCap() != 0 {
		t.Fatalf("partial write: %v", err)
	}
	if stream.closes.Load() != 1 {
		t.Fatal("partial stream not closed")
	}
	if _, err := c.WriteTo([]byte("retry"), nil); err != io.ErrShortWrite || stream.writes != 1 {
		t.Fatal("retried partial stream")
	}
}
