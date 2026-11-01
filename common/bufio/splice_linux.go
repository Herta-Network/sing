package bufio

import (
	"github.com/metacubex/sing/common/buf"
	"io"
	"runtime"
	"syscall"

	E "github.com/metacubex/sing/common/exceptions"
	N "github.com/metacubex/sing/common/network"

	"golang.org/x/sys/unix"
)

const maxSpliceSlots = 64

var spliceSlots = make(chan struct{}, spliceSlotLimit())

func spliceSlotLimit() int {
	limit := runtime.GOMAXPROCS(0) * 2
	if limit < 4 {
		return 4
	}
	if limit > maxSpliceSlots {
		return maxSpliceSlots
	}
	return limit
}
func tryAcquireSpliceSlot() bool {
	select {
	case spliceSlots <- struct{}{}:
		return true
	default:
		return false
	}
}
func releaseSpliceSlot() { <-spliceSlots }

// Each relay holds one pipe pair. Saturation falls back before consuming input.
func splice(source syscall.RawConn, destination syscall.RawConn, readCounters []N.CountFunc, writeCounters []N.CountFunc, beforeTransfer TransferHook, transferSize TransferSizeHook) (handled bool, n int64, err error) {
	if !tryAcquireSpliceSlot() {
		return false, 0, nil
	}
	defer releaseSpliceSlot()
	var pipeFDs [2]int
	if err = unix.Pipe2(pipeFDs[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		return false, 0, nil
	}
	defer unix.Close(pipeFDs[0])
	defer unix.Close(pipeFDs[1])
	handled = true
	_, _ = unix.FcntlInt(uintptr(pipeFDs[0]), unix.F_SETPIPE_SZ, maxSpliceSize)
	if transferSize == nil && beforeTransfer != nil {
		transferSize = func() int { return buf.BufferSize }
	}
	var readN, readSize, writeSize int
	var readErr, writeErr error
	readFunc := func(fd uintptr) bool {
		for {
			rn, re := unix.Splice(int(fd), nil, pipeFDs[1], nil, readSize, unix.SPLICE_F_NONBLOCK)
			readN, readErr = int(rn), re
			if re == unix.EINTR {
				continue
			}
			return re != unix.EAGAIN
		}
	}
	writeFunc := func(fd uintptr) bool {
		for writeSize > 0 {
			wn, we := unix.Splice(pipeFDs[0], nil, int(fd), nil, writeSize, unix.SPLICE_F_NONBLOCK|unix.SPLICE_F_MOVE)
			if wn > 0 {
				writeSize -= int(wn)
			}
			writeErr = we
			if we == unix.EINTR {
				continue
			}
			if we != nil {
				return we != unix.EAGAIN
			}
			if wn == 0 {
				writeErr = io.ErrNoProgress
				return true
			}
		}
		return true
	}
	for {
		readSize = resolveTransferSize(transferSize)
		readN, readErr = 0, nil
		err = source.Read(readFunc)
		if err != nil {
			readErr = err
		}
		if readErr != nil {
			if n == 0 && (readErr == unix.EINVAL || readErr == unix.ENOSYS) {
				return false, 0, nil
			}
			return true, n, E.Cause(readErr, "splice read")
		}
		if readN == 0 {
			return true, n, nil
		}
		if beforeTransfer != nil {
			if err = beforeTransfer(int64(readN)); err != nil {
				return true, n, err
			}
		}
		writeSize, writeErr = readN, nil
		err = destination.Write(writeFunc)
		transferred := readN - writeSize
		n += int64(transferred)
		if transferred > 0 {
			countTransfer(transferred, readCounters, writeCounters)
		}
		if err != nil {
			writeErr = err
		}
		if writeErr != nil {
			return true, n, E.Cause(writeErr, "splice write")
		}
	}
}
