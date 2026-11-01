package network

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

type VectorisedWriter interface {
	WriteVectorised(buffers []*buf.Buffer) error
}

type VectorisedWriteCreator interface {
	CreateVectorisedWriter() (VectorisedWriter, bool)
}

type PacketBatchWriter interface {
	// WritePacketBatch consumes every buffer on both success and failure.
	// The slices themselves are borrowed only for the duration of the call.
	WritePacketBatch([]*buf.Buffer, []M.Socksaddr) error
}

type PacketBatchWriteCreator interface {
	CreatePacketBatchWriter() (PacketBatchWriter, bool)
}

type ConnectedPacketBatchWriter interface {
	// Buffers transfer to the writer; the slice must not be retained.
	WriteConnectedPacketBatch([]*buf.Buffer) error
}

type ConnectedPacketBatchWriteCreator interface {
	CreateConnectedPacketBatchWriter() (ConnectedPacketBatchWriter, bool)
}

type VectorisedPacketWriter interface {
	WriteVectorisedPacket(buffers []*buf.Buffer, destination M.Socksaddr) error
}
