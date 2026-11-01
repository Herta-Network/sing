package uot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type memoryPacketStream struct {
	net.Conn
	input  *bytes.Reader
	output bytes.Buffer
	short  bool
	writes int
	closes atomic.Int32
}

func (c *memoryPacketStream) Read(p []byte) (int, error) {
	if c.input == nil {
		return 0, io.EOF
	}
	return c.input.Read(p)
}
func (c *memoryPacketStream) Write(p []byte) (int, error) {
	c.writes++
	if c.short {
		return 1, nil
	}
	return c.output.Write(p)
}
func (c *memoryPacketStream) Close() error { c.closes.Add(1); return nil }

func framedPackets(payloads ...[]byte) []byte {
	var result bytes.Buffer
	for _, payload := range payloads {
		binary.Write(&result, binary.BigEndian, uint16(len(payload)))
		result.Write(payload)
	}
	return result.Bytes()
}

func TestConnWriteToCountsOnlyPayloadAndStopsAfterShortWrite(t *testing.T) {
	stream := &memoryPacketStream{}
	c := NewConn(stream, Request{IsConnect: true})
	n, err := c.WriteTo([]byte("abc"), nil)
	if n != 3 || err != nil || !bytes.Equal(stream.output.Bytes(), framedPackets([]byte("abc"))) {
		t.Fatalf("write n=%d err=%v wire=%x", n, err, stream.output.Bytes())
	}
	stream.short = true
	n, err = c.WriteTo([]byte("later"), nil)
	if n != 0 || !errors.Is(err, io.ErrShortWrite) || stream.closes.Load() != 1 {
		t.Fatalf("partial frame n=%d err=%v closes=%d", n, err, stream.closes.Load())
	}
	writes := stream.writes
	if _, err = c.WriteTo([]byte("retry"), nil); !errors.Is(err, io.ErrShortWrite) || stream.writes != writes {
		t.Fatal("continued writing after a partial frame")
	}
}

func TestConnWritePacketConsumesValidationFailures(t *testing.T) {
	stream := &memoryPacketStream{}
	c := NewConn(stream, Request{})
	for _, size := range []int{4, 65536} {
		buffer := buf.NewSize(size)
		buffer.Extend(size)
		if err := c.WritePacket(buffer, M.Socksaddr{}); err == nil {
			t.Fatal("invalid packet accepted")
		}
		if buffer.RawCap() != 0 {
			t.Fatal("validation failure retained input buffer")
		}
	}
	if stream.output.Len() != 0 || stream.closes.Load() != 0 {
		t.Fatal("validation failure wrote or closed an untouched stream")
	}
}

func TestConnBatchShortWriteStopsFraming(t *testing.T) {
	stream := &memoryPacketStream{short: true}
	c := NewConn(stream, Request{IsConnect: true})
	buffers := []*buf.Buffer{buf.NewSize(2), buf.NewSize(3)}
	for _, buffer := range buffers {
		buffer.Extend(buffer.FreeLen())
	}
	err := c.WritePacketBatch(buffers, []M.Socksaddr{{}, {}})
	if !errors.Is(err, io.ErrShortWrite) || stream.closes.Load() != 1 {
		t.Fatalf("batch short write=%v closes=%d", err, stream.closes.Load())
	}
	for _, buffer := range buffers {
		if buffer.RawCap() != 0 {
			t.Fatal("failed batch retained input")
		}
	}
}

func TestConnWaitReadPacketPreservesLargeAndEmptyFrames(t *testing.T) {
	payload := bytes.Repeat([]byte{42}, 40000)
	stream := &memoryPacketStream{input: bytes.NewReader(framedPackets(payload, nil))}
	c := NewConn(stream, Request{IsConnect: true})
	c.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 16, MaxBufferSize: 128})
	for _, want := range [][]byte{payload, nil} {
		packet, _, err := c.WaitReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(packet.Bytes(), want) || packet.Start() != 8 || packet.FreeLen() < 16 {
			t.Fatal("framed datagram was truncated or lost headroom")
		}
		packet.Release()
	}
}

func TestConnShortReadBufferDrainsFrame(t *testing.T) {
	for _, extended := range []bool{false, true} {
		stream := &memoryPacketStream{input: bytes.NewReader(framedPackets([]byte("abcdef"), []byte("next")))}
		c := NewConn(stream, Request{IsConnect: true})
		var err error
		if extended {
			buffer := buf.NewSize(2)
			_, err = c.ReadPacket(buffer)
			buffer.Release()
		} else {
			_, _, err = c.ReadFrom(make([]byte, 2))
		}
		if !errors.Is(err, io.ErrShortBuffer) {
			t.Fatalf("short frame error=%v", err)
		}
		packet, _, err := c.WaitReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		if string(packet.Bytes()) != "next" {
			t.Fatal("short read desynchronized the next frame")
		}
		packet.Release()
	}
}

func TestConnConcurrentBatchesAndScratchBounds(t *testing.T) {
	stream := &memoryPacketStream{}
	c := NewConn(stream, Request{IsConnect: true})
	const workers = 4
	const packets = 130
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			buffers := make([]*buf.Buffer, packets)
			destinations := make([]M.Socksaddr, packets)
			for index := range buffers {
				buffers[index] = buf.NewSize(2)
				buffers[index].Write([]byte{byte(worker), byte(index)})
			}
			if err := c.WritePacketBatch(buffers, destinations); err != nil {
				t.Error(err)
			}
			for _, buffer := range buffers {
				if buffer.RawCap() != 0 {
					t.Error("batch retained payload")
				}
			}
		}()
	}
	group.Wait()
	reader := bytes.NewReader(stream.output.Bytes())
	seen := make(map[[2]byte]bool)
	for reader.Len() > 0 {
		var size uint16
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			t.Fatal(err)
		}
		if size != 2 {
			t.Fatalf("interleaved frame size=%d", size)
		}
		var payload [2]byte
		if _, err := io.ReadFull(reader, payload[:]); err != nil {
			t.Fatal(err)
		}
		if seen[payload] {
			t.Fatal("duplicated frame")
		}
		seen[payload] = true
	}
	if len(seen) != workers*packets {
		t.Fatal("lost batch frames")
	}
	if cap(c.batchHeaders) > bufio.MaxReadBatchSize || cap(c.batchVectors) > 2*bufio.MaxReadBatchSize {
		t.Fatal("scratch capacity grew with entire caller batch")
	}
	for _, buffer := range c.batchVectors[:cap(c.batchVectors)] {
		if buffer != nil {
			t.Fatal("scratch retained a transferred buffer")
		}
	}
}
