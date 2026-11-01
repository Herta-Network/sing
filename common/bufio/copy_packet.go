package bufio

import (
	"context"
	"errors"
	"github.com/metacubex/sing/common"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/task"
)

func CopyPacket(destinationConn N.PacketWriter, source N.PacketReader) (n int64, err error) {
	return CopyPacketWithOptions(destinationConn, source, PacketCopyOptions{})
}

func CopyPacketWithOptions(destinationConn N.PacketWriter, source N.PacketReader, options PacketCopyOptions) (n int64, err error) {
	var readCounters, writeCounters []N.CountFunc
	hadPacket := false
	originSource := source
	for {
		source, readCounters = N.UnwrapCountPacketReader(source, readCounters)
		destinationConn, writeCounters = N.UnwrapCountPacketWriter(destinationConn, writeCounters)
		if cachedReader, isCached := source.(N.CachedPacketReader); isCached {
			packet := cachedReader.ReadCachedPacket()
			if packet != nil {
				var cachedN int64
				cachedN, err = writePacketWithPool(originSource, destinationConn, []*N.PacketBuffer{packet}, readCounters, writeCounters, hadPacket)
				n += cachedN
				if err != nil {
					return
				}
				hadPacket = true
				continue
			}
		}
		break
	}
	copeN, err := copyPacketWithCounters(destinationConn, source, originSource, readCounters, writeCounters, hadPacket, packetBatchSize(options.BatchSize))
	n += copeN
	return
}

type packetCopySession struct {
	destination   N.PacketWriter
	source        N.PacketReader
	originSource  N.PacketReader
	readCounters  []N.CountFunc
	writeCounters []N.CountFunc
	handshake     N.HandshakeState
	upgradable    bool
	batchSize     int
	transferred   bool
}

func newPacketCopySession(destination N.PacketWriter, source N.PacketReader, originSource N.PacketReader, readCounters []N.CountFunc, writeCounters []N.CountFunc) *packetCopySession {
	session := &packetCopySession{
		destination:   destination,
		source:        source,
		originSource:  originSource,
		readCounters:  readCounters,
		writeCounters: writeCounters,
		batchSize:     DefaultPacketReadBatchSize,
	}
	session.ResetHandshake()
	return session
}

func (s *packetCopySession) ResetHandshake() {
	s.handshake = N.NewPacketHandshakeState(s.source, s.destination)
	s.upgradable = s.handshake.Upgradable()
}

func (s *packetCopySession) Transfer(n int64) error {
	s.transferred = true
	for _, counter := range s.readCounters {
		counter(n)
	}
	for _, counter := range s.writeCounters {
		counter(n)
	}
	if s.upgradable {
		return s.handshake.Check()
	}
	return nil
}

func (s *packetCopySession) TransferBatch(dataLens []int) error {
	if len(dataLens) > 0 {
		s.transferred = true
	}
	for _, dataLen := range dataLens {
		n := int64(dataLen)
		for _, counter := range s.readCounters {
			counter(n)
		}
		for _, counter := range s.writeCounters {
			counter(n)
		}
	}
	if s.upgradable {
		return s.handshake.Check()
	}
	return nil
}

func CopyPacketWithCounters(destinationConn N.PacketWriter, source N.PacketReader, originSource N.PacketReader, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	return copyPacketWithCounters(destinationConn, source, originSource, readCounters, writeCounters, false, DefaultPacketReadBatchSize)
}

func copyPacketWithCounters(destinationConn N.PacketWriter, source N.PacketReader, originSource N.PacketReader, readCounters []N.CountFunc, writeCounters []N.CountFunc, notFirstTime bool, batchSize int) (n int64, err error) {
	sourceReader := source
	destinationWriter := destinationConn
	session := newPacketCopySession(destinationWriter, sourceReader, originSource, readCounters, writeCounters)
	session.batchSize = batchSize
	refreshUnwrap := func() {
		sourceReader, readCounters = N.UnwrapCountPacketReader(sourceReader, readCounters)
		destinationWriter, writeCounters = N.UnwrapCountPacketWriter(destinationWriter, writeCounters)
		session.source = sourceReader
		session.destination = destinationWriter
		session.readCounters = readCounters
		session.writeCounters = writeCounters
		session.ResetHandshake()
	}
	for {
		var copyN int64
		copyN, err = copyPacketWithCountersOnce(session, notFirstTime || session.transferred)
		n += copyN
		if errors.Is(err, N.ErrHandshakeCompleted) {
			refreshUnwrap()
			continue
		}
		return
	}
}

func copyPacketWithCountersOnce(session *packetCopySession, notFirstTime bool) (n int64, err error) {
	var (
		handled bool
		copeN   int64
	)
	source := session.source
	destinationConn := session.destination
	if session.batchSize > 1 {
		batchWriter, isBatchWriter := CreatePacketBatchWriterWithCount(destinationConn)
		var connectedBatchWriter N.ConnectedPacketBatchWriterWithCount
		var isConnectedBatchWriter bool
		if !isBatchWriter {
			connectedBatchWriter, isConnectedBatchWriter = CreateConnectedPacketBatchWriterWithCount(destinationConn)
		}
		if !isBatchWriter && !isConnectedBatchWriter {
			// An opaque error-only batch method cannot preserve the copy result or
			// counters on failure. Keep batched reads and write each packet normally.
			batchWriter, isBatchWriter = &fallbackPacketBatchWriter{destinationConn}, true
		}
		batchReadWaiter, isBatchReadWaiter := CreatePacketBatchReadWaiter(source)
		if isBatchReadWaiter {
			defer releaseReadWaiter(batchReadWaiter)
			if isBatchWriter {
				readWaitOptions := N.NewReadWaitOptions(source, destinationConn)
				readWaitOptions.BatchSize = session.batchSize
				needCopy := batchReadWaiter.InitializeReadWaiter(readWaitOptions)
				if !needCopy || common.LowMemory {
					handled, copeN, err = copyPacketBatchWaitWithPool(session, batchWriter, batchReadWaiter, notFirstTime)
					if handled {
						n += copeN
						return
					}
				}
			}
			if isConnectedBatchWriter {
				readWaitOptions := N.NewReadWaitOptions(source, destinationConn)
				readWaitOptions.BatchSize = session.batchSize
				needCopy := batchReadWaiter.InitializeReadWaiter(readWaitOptions)
				if !needCopy || common.LowMemory {
					handled, copeN, err = copyPacketBatchToConnectedWaitWithPool(session, connectedBatchWriter, batchReadWaiter, notFirstTime)
					if handled {
						n += copeN
						return
					}
				}
			}
		}
		connectedBatchReadWaiter, isConnectedBatchReadWaiter := CreateConnectedPacketBatchReadWaiter(source)
		if isConnectedBatchReadWaiter {
			defer releaseReadWaiter(connectedBatchReadWaiter)
			if isBatchWriter {
				readWaitOptions := N.NewReadWaitOptions(source, destinationConn)
				readWaitOptions.BatchSize = session.batchSize
				needCopy := connectedBatchReadWaiter.InitializeReadWaiter(readWaitOptions)
				if !needCopy || common.LowMemory {
					handled, copeN, err = copyConnectedPacketBatchWaitWithPool(session, batchWriter, connectedBatchReadWaiter, notFirstTime)
					if handled {
						n += copeN
						return
					}
				}
			}
			if isConnectedBatchWriter {
				readWaitOptions := N.NewReadWaitOptions(source, destinationConn)
				readWaitOptions.BatchSize = session.batchSize
				needCopy := connectedBatchReadWaiter.InitializeReadWaiter(readWaitOptions)
				if !needCopy || common.LowMemory {
					handled, copeN, err = copyConnectedPacketBatchToConnectedWaitWithPool(session, connectedBatchWriter, connectedBatchReadWaiter, notFirstTime)
					if handled {
						n += copeN
						return
					}
				}
			}
		}
	}
	readWaiter, isReadWaiter := CreatePacketReadWaiter(source)
	if isReadWaiter {
		defer releaseReadWaiter(readWaiter)
		needCopy := readWaiter.InitializeReadWaiter(N.NewReadWaitOptions(source, destinationConn))
		if !needCopy || common.LowMemory {
			handled, copeN, err = copyPacketWaitWithPool(session, destinationConn, readWaiter, notFirstTime)
			if handled {
				n += copeN
				return
			}
		}
	}
	copeN, err = copyPacketWithPool(session, destinationConn, source, notFirstTime)
	n += copeN
	return
}

func CopyPacketWithPool(originSource N.PacketReader, destination N.PacketWriter, source N.PacketReader, readCounters []N.CountFunc, writeCounters []N.CountFunc, notFirstTime bool) (n int64, err error) {
	session := newPacketCopySession(destination, source, originSource, readCounters, writeCounters)
	return copyPacketWithPool(session, destination, source, notFirstTime)
}

func copyPacketWithPool(session *packetCopySession, destination N.PacketWriter, source N.PacketReader, notFirstTime bool) (n int64, err error) {
	options := N.NewReadWaitOptions(source, destination)
	var destinationAddress M.Socksaddr
	for {
		buffer := options.NewPacketBuffer()
		destinationAddress, err = source.ReadPacket(buffer)
		if err != nil {
			buffer.Release()
			return
		}
		dataLen := buffer.Len()
		options.PostReturn(buffer)
		err = destination.WritePacket(buffer, destinationAddress)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		n += int64(dataLen)
		if err = session.Transfer(int64(dataLen)); err != nil {
			return
		}
		notFirstTime = true
	}
}

func WritePacketWithPool(originSource N.PacketReader, destination N.PacketWriter, packetBuffers []*N.PacketBuffer, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	return writePacketWithPool(originSource, destination, packetBuffers, readCounters, writeCounters, false)
}

func writePacketWithPool(originSource N.PacketReader, destination N.PacketWriter, packetBuffers []*N.PacketBuffer, readCounters []N.CountFunc, writeCounters []N.CountFunc, notFirstTime bool) (n int64, err error) {
	options := N.NewReadWaitOptions(nil, destination)
	for index, packetBuffer := range packetBuffers {
		buffer := options.Copy(packetBuffer.Buffer)
		dataLen := buffer.Len()
		err = destination.WritePacket(buffer, packetBuffer.Destination)
		N.PutPacketBuffer(packetBuffer)
		if err != nil {
			N.ReleaseMultiPacketBuffer(packetBuffers[index+1:])
			buffer.Leak()
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		n += int64(dataLen)
		notFirstTime = true
	}
	return
}

func CopyPacketConn(ctx context.Context, source N.PacketConn, destination N.PacketConn) error {
	var group task.Group
	group.Append("upload", func(ctx context.Context) error {
		return common.Error(CopyPacket(destination, source))
	})
	group.Append("download", func(ctx context.Context) error {
		return common.Error(CopyPacket(source, destination))
	})
	group.Cleanup(func() {
		common.Close(source, destination)
	})
	group.FastFail()
	return group.Run(ctx)
}
