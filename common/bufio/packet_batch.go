package bufio

import (
	"os"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

const DefaultPacketReadBatchSize = 64

// PacketCopyOptions controls the maximum packets requested per read. Zero
// selects the Polaris-compatible batch of 64; one keeps scalar reads. Stream
// buffer limits do not apply to datagrams, which must retain packet boundaries.
type PacketCopyOptions struct{ BatchSize int }

func packetBatchSize(size int) int {
	if size <= 0 {
		size = DefaultPacketReadBatchSize
	}
	return normalizeReadBatchSize(size)
}

func minPacketInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// clearPacketSlice is the Go 1.20 equivalent of clear for scratch slices.
func clearPacketSlice[T any](values []T) {
	var zero T
	for index := range values {
		values[index] = zero
	}
}

func CreatePacketBatchWriter(writer N.PacketWriter) (N.PacketBatchWriter, bool) {
	if batchCreator, isBatchCreator := writer.(N.PacketBatchWriteCreator); isBatchCreator {
		return batchCreator.CreatePacketBatchWriter()
	}
	if batchWriter, isBatchWriter := writer.(N.PacketBatchWriter); isBatchWriter {
		return batchWriter, true
	}
	if batchWriter, created := createSyscallPacketBatchWriter(writer); created {
		return batchWriter, true
	}
	if u, ok := writer.(N.WriterWithUpstream); ok && u.WriterReplaceable() {
		if u, ok := writer.(N.WithUpstreamWriter); ok {
			return CreatePacketBatchWriter(u.UpstreamWriter().(N.PacketWriter))
		}
		if u, ok := writer.(common.WithUpstream); ok {
			return CreatePacketBatchWriter(u.Upstream().(N.PacketWriter))
		}
	}
	return nil, false
}

func CreateConnectedPacketBatchWriter(writer N.PacketWriter) (N.ConnectedPacketBatchWriter, bool) {
	if batchCreator, isBatchCreator := writer.(N.ConnectedPacketBatchWriteCreator); isBatchCreator {
		return batchCreator.CreateConnectedPacketBatchWriter()
	}
	if batchWriter, isBatchWriter := writer.(N.ConnectedPacketBatchWriter); isBatchWriter {
		return batchWriter, true
	}
	if batchWriter, created := createSyscallConnectedPacketBatchWriter(writer); created {
		return batchWriter, true
	}
	if u, ok := writer.(N.WriterWithUpstream); !ok || !u.WriterReplaceable() {
		return nil, false
	}
	if u, ok := writer.(N.WithUpstreamWriter); ok {
		return CreateConnectedPacketBatchWriter(u.UpstreamWriter().(N.PacketWriter))
	}
	if u, ok := writer.(common.WithUpstream); ok {
		return CreateConnectedPacketBatchWriter(u.Upstream().(N.PacketWriter))
	}
	return nil, false
}

func NewPacketBatchWriter(writer N.PacketWriter) N.PacketBatchWriter {
	if batchWriter, created := CreatePacketBatchWriter(writer); created {
		return batchWriter
	}
	return &fallbackPacketBatchWriter{writer}
}

func NewConnectedPacketBatchWriter(writer N.PacketWriter) N.ConnectedPacketBatchWriter {
	if batchWriter, created := CreateConnectedPacketBatchWriter(writer); created {
		return batchWriter
	}
	return &fallbackConnectedPacketBatchWriter{writer}
}

type fallbackPacketBatchWriter struct {
	writer N.PacketWriter
}

func (w *fallbackPacketBatchWriter) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return os.ErrInvalid
	}
	for index, buffer := range buffers {
		err := w.writer.WritePacket(buffer, destinations[index])
		if err != nil {
			buffer.Release()
			buf.ReleaseMulti(buffers[index+1:])
			return err
		}
	}
	return nil
}

type fallbackConnectedPacketBatchWriter struct {
	writer N.PacketWriter
}

func (w *fallbackConnectedPacketBatchWriter) WriteConnectedPacketBatch(buffers []*buf.Buffer) error {
	if len(buffers) == 0 {
		buf.ReleaseMulti(buffers)
		return os.ErrInvalid
	}
	for index, buffer := range buffers {
		err := w.writer.WritePacket(buffer, M.Socksaddr{})
		if err != nil {
			buffer.Release()
			buf.ReleaseMulti(buffers[index+1:])
			return err
		}
	}
	return nil
}
