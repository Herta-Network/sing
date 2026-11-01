package bufio

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/task"
)

const MaxCopyExtendedOnceTimes = 10 // max CopyExtendedOnce execute times

func Copy(destination io.Writer, source io.Reader) (n int64, err error) {
	return CopyWithOptions(destination, source, CopyOptions{})
}

// CopyWithOptions preserves Copy's wrapper, handshake and accounting behavior.
// Zero options keep the existing fast paths. Controlled reads are opt-in.
func CopyWithOptions(destination io.Writer, source io.Reader, options CopyOptions) (n int64, err error) {
	if source == nil {
		return 0, E.New("nil reader")
	} else if destination == nil {
		return 0, E.New("nil writer")
	}
	originSource := source
	var readCounters, writeCounters []N.CountFunc
	if len(options.WriteCounters) > 0 {
		writeCounters = append(writeCounters, options.WriteCounters...)
	}
	var _n int64
	possibly := MaxCopyExtendedOnceTimes
	for {
		source, readCounters = N.UnwrapCountReader(source, readCounters)
		destination, writeCounters = N.UnwrapCountWriter(destination, writeCounters)
		if cachedSrc, isCached := source.(N.CachedReader); isCached {
			cachedBuffer := cachedSrc.ReadCached()
			if cachedBuffer != nil {
				dataLen := cachedBuffer.Len()
				if err = options.beforeTransfer(dataLen); err != nil {
					cachedBuffer.Release()
					if n == 0 {
						err = N.ReportHandshakeFailure(originSource, err)
					}
					return n, err
				}
				var written int
				written, err = destination.Write(cachedBuffer.Bytes())
				if written < 0 || written > dataLen {
					written, err = 0, io.ErrShortWrite
				} else if written != dataLen && err == nil {
					err = io.ErrShortWrite
				}
				cachedBuffer.Release()
				n += int64(written)
				countTransfer(written, readCounters, writeCounters)
				if err != nil {
					return
				}
				continue
			}
		}
		replaceableReader, isReaderPossiblyReplaceable := source.(N.ReaderPossiblyReplaceable)
		replaceableWriter, isWriterPossiblyReplaceable := destination.(N.WriterPossiblyReplaceable)
		if possibly != 0 && ((isReaderPossiblyReplaceable && replaceableReader.ReaderPossiblyReplaceable()) ||
			(isWriterPossiblyReplaceable && replaceableWriter.WriterPossiblyReplaceable())) {
			possibly--
			_n, err = copyExtendedOnceWithOptions(destination, source, readCounters, writeCounters, options)
			n += _n
			if err != nil {
				if n == _n { // first time
					err = N.ReportHandshakeFailure(originSource, err)
				}
				if errors.Is(err, io.EOF) {
					err = nil
				}
				return
			}
			continue
		}
		srcSyscallConn, srcIsSyscall := source.(syscall.Conn)
		dstSyscallConn, dstIsSyscall := destination.(syscall.Conn)
		if srcIsSyscall && dstIsSyscall && options.ReadPolicy == nil {
			var handled bool
			handled, _n, err = copyDirectWithOptions(srcSyscallConn, dstSyscallConn, readCounters, writeCounters, options)
			if handled {
				n += _n
				return
			}
		}
		break
	}
	if options.controlled() {
		_n, err = copyControlled(originSource, NewExtendedWriter(destination), NewExtendedReader(source), readCounters, writeCounters, options, n > 0)
	} else {
		_n, err = CopyExtended(originSource, NewExtendedWriter(destination), NewExtendedReader(source), readCounters, writeCounters)
	}
	n += _n
	return
}

func CopyWithCounters(destination io.Writer, source io.Reader, originSource io.Reader, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	srcSyscallConn, srcIsSyscall := source.(syscall.Conn)
	dstSyscallConn, dstIsSyscall := destination.(syscall.Conn)
	if srcIsSyscall && dstIsSyscall {
		var handled bool
		handled, n, err = copyDirect(srcSyscallConn, dstSyscallConn, readCounters, writeCounters)
		if handled {
			return
		}
	}
	return CopyExtended(originSource, NewExtendedWriter(destination), NewExtendedReader(source), readCounters, writeCounters)
}

func CopyExtended(originSource io.Reader, destination N.ExtendedWriter, source N.ExtendedReader, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	frontHeadroom := N.CalculateFrontHeadroom(destination)
	rearHeadroom := N.CalculateRearHeadroom(destination)
	readWaiter, isReadWaiter := CreateReadWaiter(source)
	if isReadWaiter {
		defer releaseReadWaiter(readWaiter)
		needCopy := readWaiter.InitializeReadWaiter(N.ReadWaitOptions{
			FrontHeadroom: frontHeadroom,
			RearHeadroom:  rearHeadroom,
			MTU:           N.CalculateMTU(source, destination),
		})
		if !needCopy || common.LowMemory {
			var handled bool
			handled, n, err = copyWaitWithPool(originSource, destination, readWaiter, readCounters, writeCounters)
			if handled {
				return
			}
		}
	}
	return CopyExtendedWithPool(originSource, destination, source, readCounters, writeCounters)
}

// Deprecated: not used
func CopyExtendedBuffer(originSource io.Writer, destination N.ExtendedWriter, source N.ExtendedReader, buffer *buf.Buffer, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	buffer.IncRef()
	defer buffer.DecRef()
	frontHeadroom := N.CalculateFrontHeadroom(destination)
	rearHeadroom := N.CalculateRearHeadroom(destination)
	buffer.Resize(frontHeadroom, 0)
	buffer.Reserve(rearHeadroom)
	var notFirstTime bool
	for {
		err = source.ReadBuffer(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		buffer.OverCap(rearHeadroom)
		err = destination.WriteBuffer(buffer)
		if err != nil {
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func CopyExtendedWithPool(originSource io.Reader, destination N.ExtendedWriter, source N.ExtendedReader, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	options := N.NewReadWaitOptions(source, destination)
	var notFirstTime bool
	for {
		buffer := options.NewBuffer()
		err = source.ReadBuffer(buffer)
		if err != nil {
			buffer.Release()
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		options.PostReturn(buffer)
		err = destination.WriteBuffer(buffer)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}

func CopyConn(ctx context.Context, source net.Conn, destination net.Conn) error {
	var group task.Group
	if _, dstDuplex := common.Cast[N.WriteCloser](destination); dstDuplex {
		group.Append("upload", func(ctx context.Context) error {
			err := common.Error(Copy(destination, source))
			if err == nil {
				N.CloseWrite(destination)
			} else {
				common.Close(destination)
			}
			return err
		})
	} else {
		group.Append("upload", func(ctx context.Context) error {
			defer common.Close(destination)
			return common.Error(Copy(destination, source))
		})
	}
	if _, srcDuplex := common.Cast[N.WriteCloser](source); srcDuplex {
		group.Append("download", func(ctx context.Context) error {
			err := common.Error(Copy(source, destination))
			if err == nil {
				N.CloseWrite(source)
			} else {
				common.Close(source)
			}
			return err
		})
	} else {
		group.Append("download", func(ctx context.Context) error {
			defer common.Close(source)
			return common.Error(Copy(source, destination))
		})
	}
	group.Cleanup(func() {
		common.Close(source, destination)
	})
	return group.Run(ctx)
}
