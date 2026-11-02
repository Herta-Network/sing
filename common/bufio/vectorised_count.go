package bufio

import (
	"io"
	"net"

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
	writer io.Writer
}

func (w *countedNetVectorisedWriter) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	defer buf.ReleaseMulti(buffers)
	total := int64(buf.LenMulti(buffers))
	vectors := make(net.Buffers, len(buffers))
	for i, b := range buffers {
		vectors[i] = b.Bytes()
	}
	n, err := vectors.WriteTo(w.writer)
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
