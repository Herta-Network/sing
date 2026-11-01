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
