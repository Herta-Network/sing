package bufio

import (
	"io"
	"os"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func CreatePacketBatchWriterWithCount(writer N.PacketWriter) (N.PacketBatchWriterWithCount, bool) {
	if creator, ok := writer.(N.PacketBatchWriteCountCreator); ok {
		return creator.CreatePacketBatchWriterWithCount()
	}
	if batch, ok := writer.(N.PacketBatchWriterWithCount); ok {
		return batch, true
	}
	// A counter wrapper without its own batch capability must count through
	// its scalar method rather than disappear during capability discovery.
	if _, ok := writer.(N.PacketWriteCounter); ok {
		return nil, false
	}
	if batch, ok := createSyscallPacketBatchWriter(writer); ok {
		counted, ok := batch.(N.PacketBatchWriterWithCount)
		return counted, ok
	}
	if u, ok := writer.(N.WriterWithUpstream); ok && u.WriterReplaceable() {
		if u, ok := writer.(N.WithUpstreamWriter); ok {
			if upstream, ok := u.UpstreamWriter().(N.PacketWriter); ok {
				return CreatePacketBatchWriterWithCount(upstream)
			}
		} else if u, ok := writer.(common.WithUpstream); ok {
			if upstream, ok := u.Upstream().(N.PacketWriter); ok {
				return CreatePacketBatchWriterWithCount(upstream)
			}
		}
	}
	return nil, false
}

func CreateConnectedPacketBatchWriterWithCount(writer N.PacketWriter) (N.ConnectedPacketBatchWriterWithCount, bool) {
	if creator, ok := writer.(N.ConnectedPacketBatchWriteCountCreator); ok {
		return creator.CreateConnectedPacketBatchWriterWithCount()
	}
	if batch, ok := writer.(N.ConnectedPacketBatchWriterWithCount); ok {
		return batch, true
	}
	if _, ok := writer.(N.PacketWriteCounter); ok {
		return nil, false
	}
	if batch, ok := createSyscallConnectedPacketBatchWriter(writer); ok {
		counted, ok := batch.(N.ConnectedPacketBatchWriterWithCount)
		return counted, ok
	}
	if u, ok := writer.(N.WriterWithUpstream); ok && u.WriterReplaceable() {
		if u, ok := writer.(N.WithUpstreamWriter); ok {
			if upstream, ok := u.UpstreamWriter().(N.PacketWriter); ok {
				return CreateConnectedPacketBatchWriterWithCount(upstream)
			}
		} else if u, ok := writer.(common.WithUpstream); ok {
			if upstream, ok := u.Upstream().(N.PacketWriter); ok {
				return CreateConnectedPacketBatchWriterWithCount(upstream)
			}
		}
	}
	return nil, false
}

// Error-only batch methods cannot report partial progress. The fallback calls
// WritePacket on the original wrapper, preserving encryption and accounting.
func NewPacketBatchWriterWithCount(writer N.PacketWriter) N.PacketBatchWriterWithCount {
	if batch, ok := CreatePacketBatchWriterWithCount(writer); ok {
		return batch
	}
	return &fallbackPacketBatchWriter{writer}
}

func NewConnectedPacketBatchWriterWithCount(writer N.PacketWriter) N.ConnectedPacketBatchWriterWithCount {
	if batch, ok := CreateConnectedPacketBatchWriterWithCount(writer); ok {
		return batch
	}
	return &fallbackConnectedPacketBatchWriter{writer}
}

func (w *fallbackPacketBatchWriter) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	for i, buffer := range buffers {
		if err := w.writer.WritePacket(buffer, destinations[i]); err != nil {
			buffer.Release()
			buf.ReleaseMulti(buffers[i+1:])
			return i, err
		}
	}
	return len(buffers), nil
}

func (w *fallbackConnectedPacketBatchWriter) WriteConnectedPacketBatchCount(buffers []*buf.Buffer) (int, error) {
	if len(buffers) == 0 {
		return 0, os.ErrInvalid
	}
	for i, buffer := range buffers {
		if err := w.writer.WritePacket(buffer, M.Socksaddr{}); err != nil {
			buffer.Release()
			buf.ReleaseMulti(buffers[i+1:])
			return i, err
		}
	}
	return len(buffers), nil
}

// CheckPacketBatchCount rejects invalid implementations without inventing
// progress. Short success is an error; the known successful prefix is retained.
func CheckPacketBatchCount(written, total int, err error) (int, error) {
	if written < 0 || written > total {
		return 0, io.ErrShortWrite
	}
	if written != total && err == nil {
		err = io.ErrShortWrite
	}
	return written, err
}

func writePacketBatchCount(writer N.PacketBatchWriter, buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	if counted, ok := writer.(N.PacketBatchWriterWithCount); ok {
		n, err := counted.WritePacketBatchCount(buffers, destinations)
		return CheckPacketBatchCount(n, len(buffers), err)
	}
	if err := writer.WritePacketBatch(buffers, destinations); err != nil {
		return 0, err
	}
	return len(buffers), nil
}
func writeConnectedPacketBatchCount(writer N.ConnectedPacketBatchWriter, buffers []*buf.Buffer) (int, error) {
	if counted, ok := writer.(N.ConnectedPacketBatchWriterWithCount); ok {
		n, err := counted.WriteConnectedPacketBatchCount(buffers)
		return CheckPacketBatchCount(n, len(buffers), err)
	}
	if err := writer.WriteConnectedPacketBatch(buffers); err != nil {
		return 0, err
	}
	return len(buffers), nil
}
