//go:build linux

package bufio

import (
	"bytes"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	N "github.com/metacubex/sing/common/network"

	"github.com/stretchr/testify/require"
)

func TestCopyDirectAppliesTransferHookBeforeSpliceWrite(t *testing.T) {
	sourceWriter, sourceReader := TCPPipe(t)
	destinationWriter, destinationReader := TCPPipe(t)
	defer sourceWriter.Close()
	defer sourceReader.Close()
	defer destinationWriter.Close()
	defer destinationReader.Close()
	deadline := time.Now().Add(3 * time.Second)
	require.NoError(t, sourceWriter.SetDeadline(deadline))
	require.NoError(t, sourceReader.SetDeadline(deadline))
	require.NoError(t, destinationWriter.SetDeadline(deadline))
	require.NoError(t, destinationReader.SetDeadline(deadline))

	writeDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(sourceWriter, bytes.NewReader(make([]byte, 2*32*1024)))
		N.CloseWrite(sourceWriter)
		writeDone <- err
	}()
	readDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(destinationReader)
		readDone <- data
	}()

	denied := errors.New("denied")
	var counted int64
	var hookCalls int
	handled, n, err := copyDirectWithOptions(sourceReader.(syscall.Conn), destinationWriter.(syscall.Conn), nil, []N.CountFunc{
		func(size int64) { counted += size },
	}, CopyOptions{BeforeTransfer: func(size int64) error {
		hookCalls++
		if hookCalls > 1 {
			return denied
		}
		return nil
	}})
	require.NoError(t, destinationWriter.Close())
	require.NoError(t, <-writeDone)
	payload := <-readDone

	require.True(t, handled)
	require.ErrorIs(t, err, denied)
	require.Positive(t, n)
	require.LessOrEqual(t, n, int64(32*1024))
	require.Equal(t, 2, hookCalls)
	require.Equal(t, n, counted)
	require.Len(t, payload, int(n))
}

func TestCopyWithOptionsDynamicSpliceSize(t *testing.T) {
	sourceWriter, sourceReader := TCPPipe(t)
	destinationWriter, destinationReader := TCPPipe(t)
	defer sourceWriter.Close()
	defer sourceReader.Close()
	defer destinationWriter.Close()
	defer destinationReader.Close()
	deadline := time.Now().Add(3 * time.Second)
	for _, conn := range []net.Conn{sourceWriter, sourceReader, destinationWriter, destinationReader} {
		require.NoError(t, conn.SetDeadline(deadline))
	}
	data := bytes.Repeat([]byte("x"), 8192)
	writeDone := make(chan error, 1)
	readDone := make(chan []byte, 1)
	go func() { _, err := sourceWriter.Write(data); N.CloseWrite(sourceWriter); writeDone <- err }()
	go func() { p, _ := io.ReadAll(destinationReader); readDone <- p }()
	capSize, calls := 128, 0
	var counted int64
	n, err := CopyWithOptions(destinationWriter, sourceReader, CopyOptions{
		TransferSize: func() int { return capSize },
		BeforeTransfer: func(size int64) error {
			if size > int64(capSize) {
				t.Errorf("transfer %d exceeds cap %d", size, capSize)
			}
			calls++
			capSize = 512
			return nil
		},
		WriteCounters: []N.CountFunc{func(size int64) { counted += size }},
	})
	require.NoError(t, err)
	require.NoError(t, destinationWriter.Close())
	require.NoError(t, <-writeDone)
	require.Equal(t, data, <-readDone)
	require.Equal(t, int64(len(data)), n)
	require.Equal(t, n, counted)
	require.Greater(t, calls, 1)
	require.Empty(t, spliceSlots)
}
