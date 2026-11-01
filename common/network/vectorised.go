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

// PacketBatchWriterWithCount consumes all input buffers, including on error.
// written is the number of completely written packets at the start of the
// batch, not delivery acknowledgements. No caller may retry released buffers.
// A nil error requires written == len(buffers). Empty datagrams count as packets.
type PacketBatchWriterWithCount interface {
	WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (written int, err error)
}

type PacketBatchWriteCountCreator interface {
	CreatePacketBatchWriterWithCount() (PacketBatchWriterWithCount, bool)
}

// ConnectedPacketBatchWriterWithCount has the same ownership and prefix
// contract as PacketBatchWriterWithCount for a fixed peer.
type ConnectedPacketBatchWriterWithCount interface {
	WriteConnectedPacketBatchCount(buffers []*buf.Buffer) (written int, err error)
}

type ConnectedPacketBatchWriteCountCreator interface {
	CreateConnectedPacketBatchWriterWithCount() (ConnectedPacketBatchWriterWithCount, bool)
}

// VectorisedWriterWithCount consumes all buffers and reports input bytes
// accepted by the writer, including a successful prefix before an error.
type VectorisedWriterWithCount interface {
	WriteVectorisedCount(buffers []*buf.Buffer) (written int64, err error)
}
