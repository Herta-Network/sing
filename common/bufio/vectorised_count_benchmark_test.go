package bufio

import (
	"io"
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
)

type vectorDiscardWriter struct{}

func (vectorDiscardWriter) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkCountedVectorWriter(b *testing.B) {
	for _, native := range []bool{false, true} {
		name := "joined"
		if native {
			name = "tcp"
		}
		b.Run(name, func(b *testing.B) {
			var writer io.Writer = vectorDiscardWriter{}
			if native {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				defer listener.Close()
				client, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					b.Fatal(err)
				}
				server, err := listener.Accept()
				if err != nil {
					b.Fatal(err)
				}
				done := make(chan struct{})
				go func() { _, _ = io.Copy(io.Discard, server); close(done) }()
				defer func() { client.Close(); <-done; server.Close() }()
				writer = client
			}
			counted := NewVectorisedWriterWithCount(writer)
			buffers := make([]*buf.Buffer, 4)
			for i := range buffers {
				buffers[i] = buf.As(make([]byte, 1200))
			}
			write := func() {
				if n, err := counted.WriteVectorisedCount(buffers); n != 4800 || err != nil {
					b.Fatalf("n=%d err=%v", n, err)
				}
			}
			write()
			b.ReportAllocs()
			b.SetBytes(4800)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				write()
			}
			b.StopTimer()
		})
	}
}
