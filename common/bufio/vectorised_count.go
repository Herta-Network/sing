package bufio

import (
	"io"
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

// NewVectorisedWriterWithCount never infers progress from an error-only writer
// or unwraps a protocol. Standard sockets retain net.Buffers' native writev path;
// other writers receive one joined Write, whose byte count preserves framing.
func NewVectorisedWriterWithCount(writer io.Writer) N.VectorisedWriterWithCount {
	if counted, ok := writer.(N.VectorisedWriterWithCount); ok {
		return counted
	}
	switch writer.(type) {
	case *net.TCPConn, *net.UnixConn:
		return &countedNetVectorisedWriter{writer: writer}
	default:
		return &BufferedVectorisedWriter{upstream: writer}
	}
}

type countedNetVectorisedWriter struct {
	writer  io.Writer
	access  sync.Mutex
	inline  [4][]byte
	storage net.Buffers
	vectors net.Buffers
}

func (w *countedNetVectorisedWriter) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(buffers)
	total := int64(buf.LenMulti(buffers))
	// A counted writer may be called concurrently just like its net.Conn.
	// Serialize only its reusable descriptor storage and the associated write.
	w.access.Lock()
	defer w.access.Unlock()
	if w.storage == nil {
		w.storage = w.inline[:]
	}
	if len(buffers) > len(w.storage) {
		w.storage = make(net.Buffers, len(buffers))
	}
	for i, buffer := range buffers {
		w.storage[i] = buffer.Bytes()
	}
	w.vectors = w.storage[:len(buffers)]
	n, err := w.vectors.WriteTo(w.writer)
	// net.Buffers advances its slice and elements, including on partial writes.
	// Clear the original span so no consumed payload remains reachable here.
	for i := range buffers {
		w.storage[i] = nil
	}
	w.vectors = nil
	return checkVectorisedCount(n, total, err)
}

func (w *BufferedVectorisedWriter) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(buffers)
	total := buf.LenMulti(buffers)
	var data []byte
	if len(buffers) == 1 {
		data = buffers[0].Bytes()
	} else if total > 0 {
		data = buf.Get(total)
		if data != nil {
			defer buf.Put(data)
		} else {
			data = make([]byte, total)
		}
		buf.CopyMulti(data, buffers)
	}
	n, err := w.upstream.Write(data)
	return checkVectorisedCount(int64(n), int64(total), err)
}

func checkVectorisedCount(written, total int64, err error) (int64, error) {
	if written < 0 || written > total {
		return 0, io.ErrShortWrite
	}
	if written != total && err == nil {
		err = io.ErrShortWrite
	}
	return written, err
}
