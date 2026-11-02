package bufio

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
)

type retainedVectorWriter struct {
	short bool
	data  []byte
}

func (w *retainedVectorWriter) Write(p []byte) (int, error) {
	if w.short {
		return 1, io.ErrClosedPipe
	}
	w.data = append(w.data, p...)
	return len(p), nil
}

func TestCountedVectorReleasesBuffersAfterPartialWrite(t *testing.T) {
	raw := &retainedVectorWriter{short: true}
	w := &countedNetVectorisedWriter{writer: raw}
	for _, count := range []int{4, 9, 2} {
		buffers := make([]*buf.Buffer, count)
		for i := range buffers {
			buffers[i] = buf.NewSize(2)
			buffers[i].WriteString("ab")
		}
		n, err := w.WriteVectorisedCount(buffers)
		if n != 1 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("n=%d err=%v", n, err)
		}
		for _, buffer := range buffers {
			if buffer.RawCap() != 0 {
				t.Fatal("failed write retained buffer")
			}
		}
	}
	raw.short = false
	if n, err := w.WriteVectorisedCount(testBuffers("fresh", "data")); n != 9 || err != nil || string(raw.data) != "freshdata" {
		t.Fatalf("reused writer n=%d err=%v data=%q", n, err, raw.data)
	}
}

func TestCountedVectorConcurrentCalls(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	server.SetDeadline(time.Now().Add(3 * time.Second))
	w := NewVectorisedWriterWithCount(client)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(value byte) {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				first, second := buf.NewSize(1), buf.NewSize(1)
				first.WriteByte(value)
				second.WriteByte(value)
				if n, err := w.WriteVectorisedCount([]*buf.Buffer{first, second}); n != 2 || err != nil {
					t.Errorf("n=%d err=%v", n, err)
					return
				}
			}
		}(byte(worker))
	}
	data := make([]byte, 800)
	_, err = io.ReadFull(server, data)
	workers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(data); i += 2 {
		if data[i] != data[i+1] {
			t.Fatal("concurrent vectors interleaved")
		}
	}
}

func TestCountedVectorJoinedSteadyAllocations(t *testing.T) {
	w := NewVectorisedWriterWithCount(vectorDiscardWriter{})
	buffers := []*buf.Buffer{buf.As(make([]byte, 1200)), buf.As(make([]byte, 1200))}
	if allocations := testing.AllocsPerRun(100, func() {
		if n, err := w.WriteVectorisedCount(buffers); n != 2400 || err != nil {
			panic("discard write failed")
		}
	}); allocations != 0 {
		t.Fatalf("steady joined allocations=%g", allocations)
	}
}
