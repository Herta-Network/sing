package uot

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type discardPacketStream struct{ net.Conn }

func (*discardPacketStream) Write(p []byte) (int, error) { return len(p), nil }
func (*discardPacketStream) Close() error                { return nil }

// Includes the producer's owned buffer allocation, framing and downstream write.
func BenchmarkOwnedFrames(b *testing.B) {
	for _, connect := range []bool{true, false} {
		for _, headroom := range []int{0, 261} {
			for _, count := range []int{1, 8} {
				b.Run(fmt.Sprintf("connect=%v/headroom=%d/batch=%d", connect, headroom, count), func(b *testing.B) {
					c := NewConn(&discardPacketStream{}, Request{IsConnect: connect})
					packets := make([]*buf.Buffer, count)
					destinations := make([]M.Socksaddr, count)
					for i := range destinations {
						destinations[i] = M.ParseSocksaddr("example.com:443")
					}
					b.SetBytes(int64(1200 * count))
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						for i := range packets {
							packets[i] = buf.NewSize(headroom + 1200)
							packets[i].Resize(headroom, 1200)
						}
						if count == 1 {
							if err := c.WritePacket(packets[0], destinations[0]); err != nil {
								b.Fatal(err)
							}
						} else if _, err := c.WritePacketBatchCount(packets, destinations); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkOwnedTCPRoundTrip(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_, err = io.Copy(conn, conn)
		done <- err
	}()
	stream, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	stream.SetDeadline(time.Now().Add(time.Minute))
	c := NewConn(stream, Request{IsConnect: true})
	received := make([]byte, 1200)
	b.SetBytes(1200)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		packet := buf.NewSize(1202)
		packet.Resize(2, 1200)
		if err := c.WritePacket(packet, M.Socksaddr{}); err != nil {
			b.Fatal(err)
		}
		if n, _, err := c.ReadFrom(received); err != nil || n != len(received) {
			b.Fatalf("read=%d %v", n, err)
		}
	}
	b.StopTimer()
	c.Close()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}
