package bufio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
)

type ownedChunkSink struct {
	chunks  []*buf.Buffer
	failAt  int
	calls   int
	discard bool
}

func (w *ownedChunkSink) FrontHeadroom() int          { return 7 }
func (w *ownedChunkSink) RearHeadroom() int           { return 19 }
func (w *ownedChunkSink) Write(p []byte) (int, error) { return WriteBuffer(w, buf.As(p)) }
func (w *ownedChunkSink) WriteBuffer(b *buf.Buffer) error {
	w.calls++
	if w.failAt == w.calls {
		b.Release()
		return io.ErrClosedPipe
	}
	if b.Start() < 7 || b.FreeLen() < 19 {
		panic("missing framing space")
	}
	copy(b.ExtendHeader(7), "headers")
	copy(b.Extend(19), "authentication-tail")
	if w.discard {
		b.Release()
	} else {
		w.chunks = append(w.chunks, b)
	}
	return nil
}

func TestChunkWriterOwnedLifetimeAndFailure(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 1025, 4099} {
		for _, failAt := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("size=%d/fail=%d", size, failAt), func(t *testing.T) {
				want := make([]byte, size)
				for i := range want {
					want[i] = byte(i)
				}
				original := buf.NewSize(size + 26)
				original.Resize(7, 0)
				original.Write(want)
				sink := &ownedChunkSink{failAt: failAt}
				err := NewChunkWriter(sink, 1024).WriteBuffer(original)
				defer buf.ReleaseMulti(sink.chunks)
				if failAt > 0 && sink.calls == failAt {
					if !errors.Is(err, io.ErrClosedPipe) {
						t.Fatalf("write error=%v", err)
					}
					if original.RawCap() != 0 {
						t.Fatal("failed input retained")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				var got []byte
				for _, part := range sink.chunks {
					data := part.Bytes()
					if string(data[:7]) != "headers" || string(data[len(data)-19:]) != "authentication-tail" {
						t.Fatal("overlapping chunks")
					}
					got = append(got, data[7:len(data)-19]...)
				}
				if !bytes.Equal(got, want) {
					t.Fatal("payload changed while consumer retained chunks")
				}
				if len(sink.chunks) == 0 || sink.chunks[len(sink.chunks)-1] != original {
					t.Fatal("last chunk lost ownership")
				}
			})
		}
	}
}

type shortChunkWriter struct{}

func (shortChunkWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }
func TestChunkWriterShortWrite(t *testing.T) {
	if _, err := NewChunkWriter(shortChunkWriter{}, 4).Write([]byte("01234567")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write=%v", err)
	}
}

func BenchmarkChunkOwned(b *testing.B) {
	for _, size := range []int{1200, 60000, 131072} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			w := NewChunkWriter(&ownedChunkSink{discard: true}, 16384)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				p := buf.NewSize(size + 26)
				p.Resize(7, size)
				if err := w.WriteBuffer(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
