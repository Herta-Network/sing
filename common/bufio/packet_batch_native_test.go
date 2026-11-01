//go:build linux || netbsd || darwin

package bufio

import (
	"errors"
	"io"
	"net/netip"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type retryBatchRawConn struct{ callbacks int }

func (*retryBatchRawConn) Control(func(uintptr)) error   { return nil }
func (*retryBatchRawConn) Read(func(uintptr) bool) error { return io.EOF }
func (r *retryBatchRawConn) Write(callback func(uintptr) bool) error {
	for index := 0; index < 5; index++ {
		r.callbacks++
		if callback(0) {
			return nil
		}
	}
	return errors.New("batch retry made no progress")
}

func TestNativePacketBatchPartialWriteOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		raw := &retryBatchRawConn{}
		writer := &syscallPacketBatchWriter{rawConn: raw, localAddr: netip.MustParseAddrPort("127.0.0.1:1000")}
		writer.write.initialize(raw)
		var lengths []int
		writer.write.send = func(_ int, packets []packetBatchMessage, _ int) (int, syscall.Errno) {
			lengths = append(lengths, len(packets))
			if len(lengths) == 1 {
				return 1, 0
			}
			if fail {
				return 0, syscall.EIO
			}
			if len(lengths) == 2 {
				return 0, syscall.EAGAIN
			}
			return len(packets), 0
		}
		buffers := testBuffers("a", "bb", "ccc")
		destination := M.ParseSocksaddr("127.0.0.1:1001")
		err := writer.WritePacketBatch(buffers, []M.Socksaddr{destination, destination, destination})
		if fail && !errors.Is(err, syscall.EIO) || !fail && err != nil {
			t.Fatalf("fail=%v error=%v", fail, err)
		}
		want := []int{3, 2, 2}
		if fail {
			want = []int{3, 2}
		}
		if !reflect.DeepEqual(lengths, want) {
			t.Fatalf("send progress=%v, want %v", lengths, want)
		}
		for _, buffer := range buffers {
			if buffer.RawCap() != 0 {
				t.Fatal("batch retained payload after return")
			}
		}
		if writer.write.writeMsgvec != nil {
			t.Fatal("write callback retained completed descriptors")
		}
		for _, vector := range writer.iovecs[:cap(writer.iovecs)] {
			if !reflect.ValueOf(vector).IsZero() {
				t.Fatal("cached iovec retained a payload pointer")
			}
		}
		for _, message := range writer.msgvec[:cap(writer.msgvec)] {
			if !reflect.ValueOf(message).IsZero() {
				t.Fatal("cached message retained a payload pointer")
			}
		}
	}
}

func TestNativePacketBatchReinitializeAndTruncation(t *testing.T) {
	input, output, destination := UDPPipe(t)
	defer input.Close()
	defer output.Close()
	input.SetReadDeadline(time.Now().Add(3 * time.Second))
	reader, ok := createSyscallPacketBatchReadWaiter(NewPacketConn(input))
	if !ok {
		t.Skip("native packet batch reader unavailable")
	}
	waiter := reader.(*syscallPacketBatchReadWaiter)
	defer waiter.ReleaseReadWaiter()
	waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 3, MTU: 16})
	if _, err := output.WriteTo([]byte("hello"), input.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buffers, destinations, err := waiter.WaitReadPackets()
	if err != nil || len(buffers) != 1 || destinations[0] != destination {
		t.Fatalf("batch read: count=%d err=%v", len(buffers), err)
	}
	delivered := buffers[0]
	defer delivered.Release()
	unused := append([]*buf.Buffer(nil), waiter.buffers...)
	waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 3, MTU: 2})
	for _, buffer := range unused {
		if buffer != nil && buffer.RawCap() != 0 {
			t.Fatal("old read buffer survived initialization")
		}
	}
	if string(delivered.Bytes()) != "hello" {
		t.Fatal("initialization reclaimed caller-owned payload")
	}
	for index := range waiter.buffers {
		waiter.buffers[index] = waiter.options.NewPacketBuffer()
	}
	owned := append([]*buf.Buffer(nil), waiter.buffers...)
	if _, err := output.WriteTo([]byte("large"), input.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := waiter.WaitReadPackets(); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("truncated packet error=%v", err)
	}
	for _, buffer := range owned {
		if buffer.RawCap() != 0 {
			t.Fatal("truncated read retained payload")
		}
	}
	for _, message := range waiter.msgvec {
		if !reflect.ValueOf(message).IsZero() {
			t.Fatal("failed read retained iovec pointers")
		}
	}
}

func TestNativePacketBatchInvalidDestinationReleasesPayload(t *testing.T) {
	writer := &syscallPacketBatchWriter{}
	buffer := testBuffers("payload")[0]
	if err := writer.WritePacketBatch([]*buf.Buffer{buffer}, []M.Socksaddr{{Fqdn: "invalid.example", Port: 53}}); err == nil {
		t.Fatal("native writer accepted an unresolved destination")
	}
	if buffer.RawCap() != 0 {
		t.Fatal("invalid destination retained payload")
	}
}

func TestScalarPacketWaiterRejectsTruncation(t *testing.T) {
	input, output, _ := UDPPipe(t)
	defer input.Close()
	defer output.Close()
	input.SetReadDeadline(time.Now().Add(time.Second))
	waiter, ok := createSyscallPacketReadWaiter(NewPacketConn(input))
	if !ok {
		t.Fatal("scalar packet waiter unavailable")
	}
	defer waiter.ReleaseReadWaiter()
	waiter.InitializeReadWaiter(N.ReadWaitOptions{MTU: 2})
	if _, err := output.WriteTo([]byte("large"), input.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buffer, _, err := waiter.WaitReadPacket()
	if !errors.Is(err, io.ErrShortBuffer) || buffer != nil {
		t.Fatalf("truncated scalar packet buffer=%v err=%v", buffer, err)
	}
}
