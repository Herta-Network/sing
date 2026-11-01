package uot

import (
	"errors"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type partialFrameStream struct {
	memoryPacketStream
	limit   int
	failure error
}

func (c *partialFrameStream) Write(p []byte) (int, error) {
	c.writes++
	n := len(p)
	if n > c.limit {
		n = c.limit
	}
	c.output.Write(p[:n])
	return n, c.failure
}

func TestBatchCountOnlyCompleteFrames(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		request := Request{IsConnect: true, Destination: M.ParseSocksaddr("127.0.0.1:53")}
		requestSize := 0
		if lazy {
			encoded, err := EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			requestSize = encoded.Len()
			encoded.Release()
		}
		// Empty frame (2 bytes), 'bb' (4 bytes), then each boundary in the next frame.
		for _, sent := range []int{0, 1, 2, 3, 5, 6, 7, 8, 9, 11} {
			for _, failure := range []error{nil, io.ErrClosedPipe} {
				stream := &partialFrameStream{limit: requestSize + sent, failure: failure}
				var conn *Conn
				if lazy {
					conn = NewLazyConn(stream, request)
				} else {
					conn = NewConn(stream, request)
				}
				buffers := []*buf.Buffer{buf.NewSize(1), buf.NewSize(2), buf.NewSize(3)}
				buffers[1].WriteString("bb")
				buffers[2].WriteString("ccc")
				n, err := conn.WritePacketBatchCount(buffers, make([]M.Socksaddr, 3))
				want := 0
				for _, end := range []int{2, 6, 11} {
					if sent >= end {
						want++
					}
				}
				if n != want {
					t.Fatalf("lazy=%v bytes=%d packets=%d want=%d", lazy, sent, n, want)
				}
				for _, b := range buffers {
					if b.RawCap() != 0 {
						t.Fatal("batch retained buffer")
					}
				}
				if sent < 11 || failure != nil {
					if err == nil || stream.closes.Load() == 0 {
						t.Fatalf("partial frame not poisoned: %v", err)
					}
					writes := stream.writes
					retry := buf.NewSize(8)
					retry.WriteString("retry")
					if count, retryErr := conn.WritePacketBatchCount([]*buf.Buffer{retry}, []M.Socksaddr{{}}); count != 0 || retryErr == nil || stream.writes != writes || retry.RawCap() != 0 {
						t.Fatal("retry after broken frame")
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestLazyBatchCountExcludesPartialRequest(t *testing.T) {
	stream := &partialFrameStream{limit: 1, failure: io.ErrClosedPipe}
	conn := NewLazyConn(stream, Request{IsConnect: true, Destination: M.ParseSocksaddr("127.0.0.1:53")})
	buffer := buf.NewSize(2)
	buffer.WriteString("hi")
	n, err := conn.WritePacketBatchCount([]*buf.Buffer{buffer}, []M.Socksaddr{M.ParseSocksaddr("127.0.0.1:53")})
	if n != 0 || !errors.Is(err, io.ErrClosedPipe) || buffer.RawCap() != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
