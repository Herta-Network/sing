package bufio

import (
	"errors"
	"io"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

// CopyReadOptions controls the next stream read. BatchSize <= 1 selects a
// single-buffer read; larger batches are capped at MaxReadBatchSize.
type CopyReadOptions struct {
	BatchSize int
	// MaxBufferSize bounds the requested size of each new stream buffer,
	// including headroom. Zero retains the normal buffer size. Required
	// headroom plus one payload byte is always reserved. Pools may round the
	// allocation up; cached/protocol-owned buffers are not resized by this cap.
	MaxBufferSize int
}

type CopyReadPolicy func() CopyReadOptions
type TransferHook func(int64) error

// CopyOptions is consumed synchronously by CopyWithOptions. ReadPolicy is
// evaluated between reads, and again before publishing a batch. BeforeTransfer
// may reject a write; the copy engine then releases the buffers it still owns.
// Callbacks must synchronize any state shared with other goroutines.
type CopyOptions struct {
	ReadPolicy     CopyReadPolicy
	BeforeTransfer TransferHook
	WriteCounters  []N.CountFunc
}

func (o CopyOptions) controlled() bool { return o.ReadPolicy != nil || o.BeforeTransfer != nil }

func (o CopyOptions) readOptions() CopyReadOptions {
	var result CopyReadOptions
	if o.ReadPolicy != nil {
		result = o.ReadPolicy()
	}
	result.BatchSize = normalizeReadBatchSize(result.BatchSize)
	if result.MaxBufferSize < 0 {
		result.MaxBufferSize = 0
	}
	return result
}

func (o CopyOptions) beforeTransfer(size int) error {
	if o.BeforeTransfer != nil {
		return o.BeforeTransfer(int64(size))
	}
	return nil
}

func countTransfer(size int, readCounters, writeCounters []N.CountFunc) {
	for _, counter := range readCounters {
		counter(int64(size))
	}
	for _, counter := range writeCounters {
		counter(int64(size))
	}
}

func releaseReadWaiter(waiter any) {
	if releaser, ok := waiter.(N.ReadWaiterReleaser); ok {
		releaser.ReleaseReadWaiter()
	}
}

// copyControlled keeps the existing protocol/handshake wrappers intact. A
// controlled transfer deliberately bypasses splice, whose current API cannot
// enforce a per-read policy or a pre-write admission hook.
func copyControlled(originSource io.Reader, destination N.ExtendedWriter, source N.ExtendedReader,
	readCounters, writeCounters []N.CountFunc, options CopyOptions, hasTransferred bool,
) (n int64, err error) {
	base := N.NewReadWaitOptions(source, destination)
	var reader N.ReadWaiter
	var vectorReader N.VectorisedReadWaiter
	var vectorWriter N.VectorisedWriter
	var hasReader, hasVectorReader, hasVectorWriter, checkedReader, checkedVector bool
	var singleOptions, vectorOptions N.ReadWaitOptions
	var singleActive, vectorActive, singleNeedsCopy, vectorNeedsCopy bool
	defer func() {
		if singleActive {
			releaseReadWaiter(reader)
		}
		if vectorActive {
			releaseReadWaiter(vectorReader)
		}
	}()

	writeOne := func(buffer *buf.Buffer) error {
		size := buffer.Len()
		if err := options.beforeTransfer(size); err != nil {
			buffer.Release()
			return err
		}
		if raw, ok := destination.(*ExtendedWriterWrapper); ok {
			written, err := raw.Writer.Write(buffer.Bytes())
			buffer.Release()
			if written < 0 || written > size {
				written, err = 0, io.ErrShortWrite
			} else if written != size && err == nil {
				err = io.ErrShortWrite
			}
			n += int64(written)
			countTransfer(written, readCounters, writeCounters)
			return err
		}
		if err := destination.WriteBuffer(buffer); err != nil {
			buffer.Leak()
			return err
		}
		n += int64(size)
		countTransfer(size, readCounters, writeCounters)
		return nil
	}
	for {
		policy := options.readOptions()
		requested := base
		requested.MaxBufferSize = policy.MaxBufferSize
		requested.BatchSize = policy.BatchSize
		if policy.BatchSize > 1 && !checkedVector {
			vectorReader, hasVectorReader = CreateVectorisedReadWaiter(source)
			vectorWriter, hasVectorWriter = CreateVectorisedWriter(N.UnwrapWriter(destination))
			checkedVector = true
		}
		if hasVectorReader && policy.BatchSize > 1 {
			if singleActive {
				releaseReadWaiter(reader)
				singleActive = false
			}
			if !vectorActive || vectorOptions != requested {
				vectorNeedsCopy = vectorReader.InitializeReadWaiter(requested)
				vectorOptions, vectorActive = requested, true
			}
			if !vectorNeedsCopy || common.LowMemory {
				buffers, readErr := vectorReader.WaitReadBuffers()
				if readErr != nil {
					buf.ReleaseMulti(buffers)
					if errors.Is(readErr, io.EOF) {
						return n, nil
					}
					return n, readErr
				}
				if len(buffers) == 0 || buf.LenMulti(buffers) == 0 {
					buf.ReleaseMulti(buffers)
					return n, io.ErrNoProgress
				}
				// A policy can change while Read blocks. Finish ownership of the
				// returned batch under the current policy before the next read.
				for offset := 0; offset < len(buffers); {
					current := options.readOptions()
					end := offset + current.BatchSize
					if end > len(buffers) {
						end = len(buffers)
					}
					if hasVectorWriter && end-offset > 1 {
						batch := buffers[offset:end]
						size := buf.LenMulti(batch)
						if err = options.beforeTransfer(size); err != nil {
							buf.ReleaseMulti(buffers[offset:])
							break
						}
						if err = vectorWriter.WriteVectorised(batch); err != nil {
							for _, buffer := range batch {
								buffer.Leak()
							}
							buf.ReleaseMulti(buffers[end:])
							break
						}
						n += int64(size)
						countTransfer(size, readCounters, writeCounters)
						offset = end
					} else {
						if err = writeOne(buffers[offset]); err != nil {
							buf.ReleaseMulti(buffers[offset+1:])
							break
						}
						offset++
					}
				}
				if err != nil {
					if n == 0 && !hasTransferred {
						err = N.ReportHandshakeFailure(originSource, err)
					}
					return n, err
				}
				continue
			}
		}
		if vectorActive {
			releaseReadWaiter(vectorReader)
			vectorActive = false
		}
		requested.BatchSize = 1
		var buffer *buf.Buffer
		if !checkedReader {
			reader, hasReader = CreateReadWaiter(source)
			checkedReader = true
		}
		if hasReader {
			if !singleActive || singleOptions != requested {
				singleNeedsCopy = reader.InitializeReadWaiter(requested)
				singleOptions, singleActive = requested, true
			}
		}
		if hasReader && (!singleNeedsCopy || common.LowMemory) {
			buffer, err = reader.WaitReadBuffer()
		} else {
			buffer = requested.NewBuffer()
			err = source.ReadBuffer(buffer)
			requested.PostReturn(buffer)
		}
		if err != nil {
			buffer.Release()
			if errors.Is(err, io.EOF) {
				return n, nil
			}
			return n, err
		}
		if buffer == nil || buffer.IsEmpty() {
			buffer.Release()
			return n, io.ErrNoProgress
		}
		if err = writeOne(buffer); err != nil {
			if n == 0 && !hasTransferred {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return n, err
		}
	}
}
