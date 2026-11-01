package uot

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func TestConnWritePacketBatch(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	conn := NewConn(client, Request{})
	destinations := []M.Socksaddr{
		M.ParseSocksaddr("1.1.1.1:53"),
		M.ParseSocksaddr("example.com:443"),
	}
	payloads := [][]byte{[]byte("first"), []byte("second")}
	buffers := make([]*buf.Buffer, len(payloads))
	for index, payload := range payloads {
		buffers[index] = buf.NewSize(len(payload))
		_, _ = buffers[index].Write(payload)
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- conn.WritePacketBatch(buffers, destinations) }()
	for index := range payloads {
		destination, err := AddrParser.ReadAddrPort(server)
		if err != nil {
			t.Fatal(err)
		}
		if destination != destinations[index] {
			t.Fatalf("destination=%v, want %v", destination, destinations[index])
		}
		var length uint16
		if err = binary.Read(server, binary.BigEndian, &length); err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, int(length))
		if _, err = io.ReadFull(server, payload); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, payloads[index]) {
			t.Fatalf("payload=%q, want %q", payload, payloads[index])
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestConnRejectsOversizedPacket(t *testing.T) {
	conn := &Conn{isConnect: true, writer: releaseVectorWriter{}}
	buffer := buf.NewSize(int(^uint16(0)) + 1)
	buffer.Extend(buffer.Cap())
	if err := conn.WritePacket(buffer, M.Socksaddr{}); err == nil {
		t.Fatal("oversized UoT packet was accepted")
	}
	buffer.Release()
	if _, err := conn.WriteTo(make([]byte, int(^uint16(0))+1), nil); err == nil {
		t.Fatal("oversized UoT byte slice was accepted")
	}
}

func BenchmarkConnWritePacket(b *testing.B) {
	destination := M.Socksaddr{}
	writer := releaseVectorWriter{}
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		conn := &Conn{isConnect: true, writer: writer}
		for index := 0; index < b.N; index++ {
			payload := buf.NewSize(1)
			payload.Extend(1)[0] = 1
			header := buf.NewSize(2)
			if err := binary.Write(header, binary.BigEndian, uint16(payload.Len())); err != nil {
				b.Fatal(err)
			}
			if err := legacyVectorWrite(conn, header, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("optimized", func(b *testing.B) {
		b.ReportAllocs()
		conn := NewConn(&discardPacketStream{}, Request{IsConnect: true})
		for index := 0; index < b.N; index++ {
			payload := buf.NewSize(3)
			payload.Resize(2, 1)
			payload.Bytes()[0] = 1
			if err := conn.WritePacket(payload, destination); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func legacyVectorWrite(conn *Conn, header *buf.Buffer, payload *buf.Buffer) error {
	return conn.writer.WriteVectorised([]*buf.Buffer{header, payload})
}

type releaseVectorWriter struct{}

func (releaseVectorWriter) WriteVectorised(buffers []*buf.Buffer) error {
	buf.ReleaseMulti(buffers)
	return nil
}
