package network

import (
	"bytes"
	"testing"

	"github.com/metacubex/sing/common/buf"
)

func TestReadWaitOptionsCopyPreservesLargePacket(t *testing.T) {
	payload := bytes.Repeat([]byte{42}, 20*1024)
	input := buf.NewSize(len(payload))
	input.Write(payload)
	output := (ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 16}).Copy(input)
	defer output.Release()
	if !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("copied %d bytes, want %d", output.Len(), len(payload))
	}
	if output.Start() < 8 || output.FreeLen() < 16 {
		t.Fatal("copy did not preserve required headroom")
	}
	if input.RawCap() != 0 {
		t.Fatal("copy did not release input ownership")
	}
}

func TestReadWaitOptionsStreamLimitPreservesHeadroom(t *testing.T) {
	for _, limit := range []int{4096, 16} {
		options := ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 16, MTU: 65535, MaxBufferSize: limit}
		buffer := options.NewBuffer()
		if buffer.Start() != 8 || buffer.FreeLen() < 1 {
			t.Fatal("stream limit consumed required headroom")
		}
		if limit == 4096 && buffer.Cap()+16 != limit {
			t.Fatalf("stream requested capacity=%d, want %d", buffer.Cap()+16, limit)
		}
		if limit == 16 && buffer.FreeLen() != 1 {
			t.Fatal("minimum payload should take precedence over an impossible limit")
		}
		options.PostReturn(buffer)
		if buffer.FreeLen() < 16 {
			t.Fatal("stream limit lost tail room")
		}
		buffer.Release()
	}
}

func TestReadWaitOptionsDoesNotLimitDatagrams(t *testing.T) {
	buffer := (ReadWaitOptions{MTU: 20000, MaxBufferSize: 128}).NewPacketBuffer()
	defer buffer.Release()
	if buffer.FreeLen() != 20000 {
		t.Fatal("stream buffer limit truncated datagram capacity")
	}
}

func TestStreamGrowthAndDatagramHeadroom(t *testing.T) {
	normal := (ReadWaitOptions{MTU: 65535}).NewBuffer()
	if normal.Cap() != buf.BufferSize {
		t.Fatal("stream grew without policy")
	}
	normal.Release()
	grown := (ReadWaitOptions{IncreaseBuffer: true, MaxBufferSize: 48000}).NewBuffer()
	if grown.Cap() != 48000 {
		t.Fatalf("grown cap=%d", grown.Cap())
	}
	grown.Release()
	packet := (ReadWaitOptions{FrontHeadroom: buf.UDPBufferSize + 1, RearHeadroom: 32}).NewPacketBuffer()
	if packet.FreeLen() != 1 {
		t.Fatal("packet headroom must preserve payload space")
	}
	packet.Release()
}
