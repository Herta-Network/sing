//go:build linux || darwin

package bufio

import (
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func BenchmarkPacketBatchWrite(b *testing.B) {
	source, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer source.Close()
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		buffer := make([]byte, 64*1024)
		for {
			if _, _, readErr := target.ReadFromUDPAddrPort(buffer); readErr != nil {
				return
			}
		}
	}()
	defer func() {
		target.Close()
		<-drainDone
	}()

	packetConn := NewPacketConn(source)
	syscallWriter, created := CreatePacketBatchWriter(packetConn)
	if !created {
		b.Skip("packet batch syscall backend is not available")
	}
	const batchSize = 16
	const payloadSize = 1200
	payload := make([]byte, payloadSize)
	buffers := make([]*buf.Buffer, batchSize)
	destinations := make([]M.Socksaddr, batchSize)
	destination := M.SocksaddrFromNet(target.LocalAddr())
	for index := range buffers {
		buffers[index] = buf.As(payload)
		destinations[index] = destination
	}

	run := func(b *testing.B, writer N.PacketBatchWriter) {
		b.ReportAllocs()
		b.SetBytes(batchSize * payloadSize)
		b.ReportMetric(batchSize, "datagrams/op")
		for index := 0; index < b.N; index++ {
			if err := writer.WritePacketBatch(buffers, destinations); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("syscall-batch", func(b *testing.B) { run(b, syscallWriter) })
	b.Run("packet-loop", func(b *testing.B) { run(b, &fallbackPacketBatchWriter{packetConn}) })
}
