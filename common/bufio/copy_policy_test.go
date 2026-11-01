package bufio

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

func TestCopyCountsCachedPrefix(t *testing.T) {
	prefix := buf.NewSize(6)
	prefix.WriteString("cached")
	source := NewCachedReader(bytes.NewBufferString("-stream"), prefix)
	defer source.Close()
	var output bytes.Buffer
	n, err := Copy(&output, source)
	if err != nil || n != 13 || output.String() != "cached-stream" || prefix.RawCap() != 0 {
		t.Fatalf("copy cached prefix: n=%d err=%v output=%q retained=%d", n, err, output.String(), prefix.RawCap())
	}
}

type shortPolicyWriter struct{}

func (shortPolicyWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

func TestCopyRejectsShortWrites(t *testing.T) {
	for _, cached := range []bool{false, true} {
		var source io.Reader = bytes.NewBufferString("payload")
		if cached {
			prefix := buf.NewSize(7)
			prefix.WriteString("payload")
			source = NewCachedReader(bytes.NewReader(nil), prefix)
		}
		n, err := CopyWithOptions(shortPolicyWriter{}, source, CopyOptions{ReadPolicy: func() CopyReadOptions {
			return CopyReadOptions{MaxBufferSize: 128}
		}})
		if !errors.Is(err, io.ErrShortWrite) || n != 3 {
			t.Fatalf("cached=%v short write = %d, %v", cached, n, err)
		}
	}
}

// This source implements both waiter interfaces with shared initialization
// state, exercising mode switches without allowing one mode's release to
// invalidate the other mode after it was initialized.
type policyTestReader struct {
	data        *bytes.Reader
	options     N.ReadWaitOptions
	active      bool
	reads       int
	releases    int
	created     []*buf.Buffer
	initialized []N.ReadWaitOptions
	onRead      func(int)
}

func (r *policyTestReader) Read(p []byte) (int, error) { return r.data.Read(p) }
func (r *policyTestReader) ReadBuffer(buffer *buf.Buffer) error {
	n, err := r.Read(buffer.FreeBytes())
	buffer.Truncate(n)
	return err
}
func (r *policyTestReader) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	r.options, r.active = options, true
	r.initialized = append(r.initialized, options)
	return false
}
func (r *policyTestReader) ReleaseReadWaiter() { r.active = false; r.releases++ }
func (r *policyTestReader) nextBuffer() (*buf.Buffer, error) {
	if !r.active {
		return nil, os.ErrInvalid
	}
	if r.data.Len() == 0 {
		return nil, io.EOF
	}
	buffer := r.options.NewBuffer()
	r.created = append(r.created, buffer)
	buffer.ReadOnceFrom(r.data)
	r.options.PostReturn(buffer)
	return buffer, nil
}
func (r *policyTestReader) readFinished() {
	r.reads++
	if r.onRead != nil {
		r.onRead(r.reads)
	}
}
func (r *policyTestReader) WaitReadBuffer() (*buf.Buffer, error) {
	buffer, err := r.nextBuffer()
	if err == nil {
		r.readFinished()
	}
	return buffer, err
}
func (r *policyTestReader) WaitReadBuffers() ([]*buf.Buffer, error) {
	var result []*buf.Buffer
	for index := 0; index < r.options.BatchSize; index++ {
		buffer, err := r.nextBuffer()
		if err != nil {
			if len(result) > 0 && errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		result = append(result, buffer)
	}
	r.readFinished()
	return result, nil
}

type policyTestWriter struct {
	bytes.Buffer
	singles, batches int
	err              error
}

func (*policyTestWriter) FrontHeadroom() int { return 8 }
func (*policyTestWriter) RearHeadroom() int  { return 16 }
func (w *policyTestWriter) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	w.singles++
	if buffer.Start() < 8 || buffer.FreeLen() < 16 {
		return errors.New("missing writer headroom")
	}
	if w.err != nil {
		return w.err
	}
	_, err := w.Buffer.Write(buffer.Bytes())
	return err
}
func (w *policyTestWriter) WriteVectorised(buffers []*buf.Buffer) error {
	w.batches++
	for index, buffer := range buffers {
		if err := w.WriteBuffer(buffer); err != nil {
			buf.ReleaseMulti(buffers[index+1:])
			return err
		}
	}
	return nil
}

func assertPolicyBuffersReleased(t *testing.T, reader *policyTestReader) {
	t.Helper()
	if reader.active || reader.releases == 0 {
		t.Fatal("copy did not release its read waiter")
	}
	for _, buffer := range reader.created {
		if buffer.RawCap() != 0 {
			t.Fatal("copy retained a transferred buffer")
		}
	}
}

func TestCopyPolicySwitchesBatchAndBufferLimits(t *testing.T) {
	payload := bytes.Repeat([]byte{42}, 4096)
	reader := &policyTestReader{data: bytes.NewReader(payload)}
	writer := &policyTestWriter{}
	policy := CopyReadOptions{BatchSize: 1, MaxBufferSize: 128}
	reader.onRead = func(reads int) {
		switch reads {
		case 1:
			policy = CopyReadOptions{BatchSize: 4, MaxBufferSize: 256}
		case 2:
			policy = CopyReadOptions{BatchSize: 2, MaxBufferSize: 64}
		case 3:
			policy = CopyReadOptions{BatchSize: 1, MaxBufferSize: 128}
		}
	}
	var checked, counted int64
	n, err := CopyWithOptions(writer, reader, CopyOptions{
		ReadPolicy:     func() CopyReadOptions { return policy },
		BeforeTransfer: func(size int64) error { checked += size; return nil },
		WriteCounters:  []N.CountFunc{func(size int64) { counted += size }},
	})
	if err != nil || n != int64(len(payload)) || checked != n || counted != n || !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("copy = %d, checked=%d counted=%d err=%v", n, checked, counted, err)
	}
	if writer.batches == 0 || len(reader.initialized) != 4 {
		t.Fatalf("batch path or policy transitions missing: batches=%d options=%+v", writer.batches, reader.initialized)
	}
	for index, want := range []CopyReadOptions{{1, 128}, {4, 256}, {2, 64}, {1, 128}} {
		got := reader.initialized[index]
		if got.BatchSize != want.BatchSize || got.MaxBufferSize != want.MaxBufferSize {
			t.Fatalf("read policy %d = %+v, want %+v", index, got, want)
		}
	}
	assertPolicyBuffersReleased(t, reader)
}

func TestCopyPolicyDisablesBatchAfterRead(t *testing.T) {
	payload := bytes.Repeat([]byte{42}, 512)
	reader := &policyTestReader{data: bytes.NewReader(payload)}
	writer := &policyTestWriter{}
	batch := 4
	reader.onRead = func(int) { batch = 1 }
	n, err := CopyWithOptions(writer, reader, CopyOptions{ReadPolicy: func() CopyReadOptions {
		return CopyReadOptions{BatchSize: batch, MaxBufferSize: 256}
	}})
	if err != nil || n != int64(len(payload)) || writer.batches != 0 || !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("disabled batch: n=%d batches=%d err=%v", n, writer.batches, err)
	}
	assertPolicyBuffersReleased(t, reader)
}

func TestCopyPolicyReleasesRejectedAndFailedWrites(t *testing.T) {
	for _, batch := range []int{1, 4} {
		for _, reject := range []bool{false, true} {
			reader := &policyTestReader{data: bytes.NewReader(bytes.Repeat([]byte{42}, 512))}
			writer := &policyTestWriter{}
			failure := errors.New("transfer rejected")
			var counted int64
			options := CopyOptions{
				ReadPolicy:    func() CopyReadOptions { return CopyReadOptions{BatchSize: batch, MaxBufferSize: 128} },
				WriteCounters: []N.CountFunc{func(n int64) { counted += n }},
			}
			if reject {
				options.BeforeTransfer = func(int64) error { return failure }
			} else {
				writer.err = failure
			}
			n, err := CopyWithOptions(writer, reader, options)
			if !errors.Is(err, failure) || n != 0 || counted != 0 || writer.Len() != 0 {
				t.Fatalf("batch=%d reject=%v n=%d counted=%d err=%v", batch, reject, n, counted, err)
			}
			assertPolicyBuffersReleased(t, reader)
		}
	}
}

func TestVectorReadWaiterReconfigurationOwnership(t *testing.T) {
	writer, source := TCPPipe(t)
	source.SetReadDeadline(time.Now().Add(3 * time.Second))
	waiter, ok := createVectorisedSyscallReadWaiter(source)
	if !ok {
		t.Fatal("no syscall read waiter")
	}
	defer waiter.ReleaseReadWaiter()
	waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 4, MaxBufferSize: 1024})
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	buffers, err := waiter.WaitReadBuffers()
	if err != nil || len(buffers) != 1 {
		t.Fatalf("read = %d buffers, %v", len(buffers), err)
	}
	delivered := buffers[0]
	defer delivered.Release()
	unused := append([]*buf.Buffer(nil), waiter.buffers...)
	waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 2, MaxBufferSize: 256})
	for _, buffer := range unused {
		if buffer != nil && buffer.RawCap() != 0 {
			t.Fatal("old read buffer survived reconfiguration")
		}
	}
	if string(delivered.Bytes()) != "abc" || delivered.RawCap() == 0 {
		t.Fatal("reconfiguration reclaimed caller-owned data")
	}
	waiter.ReleaseReadWaiter()
	waiter.ReleaseReadWaiter()
	if _, err := waiter.WaitReadBuffers(); !errors.Is(err, os.ErrInvalid) {
		t.Fatal("released waiter remained readable")
	}
}

func TestVectorReadWaiterCloseReleasesPendingBuffers(t *testing.T) {
	_, source := TCPPipe(t)
	waiter, ok := createVectorisedSyscallReadWaiter(source)
	if !ok {
		t.Fatal("no syscall read waiter")
	}
	defer waiter.ReleaseReadWaiter()
	waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 4, MaxBufferSize: 1024})
	// Populate owned buffers before the blocked read to verify the error path.
	waiter.prepareBuffers()
	owned := append([]*buf.Buffer(nil), waiter.buffers...)
	done := make(chan error, 1)
	go func() { _, err := waiter.WaitReadBuffers(); done <- err }()
	source.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed socket read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("socket close did not interrupt vector read")
	}
	for _, buffer := range owned {
		if buffer.RawCap() != 0 {
			t.Fatal("read error retained an owned buffer")
		}
	}
}

func TestCopyPolicyTCPAndHalfClose(t *testing.T) {
	client, source := TCPPipe(t)
	client.SetDeadline(time.Now().Add(5 * time.Second))
	source.SetDeadline(time.Now().Add(5 * time.Second))
	payload := bytes.Repeat([]byte("payload"), 20000)
	written := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.(*net.TCPConn).CloseWrite()
		}
		written <- err
	}()
	var output bytes.Buffer
	n, err := CopyWithOptions(&output, source, CopyOptions{ReadPolicy: func() CopyReadOptions {
		return CopyReadOptions{BatchSize: 4, MaxBufferSize: 4096}
	}})
	if err != nil || n != int64(len(payload)) || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("TCP copy n=%d err=%v", n, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 5)
	if _, err := io.ReadFull(client, response); err != nil || string(response) != "reply" {
		t.Fatalf("half close lost reverse direction: %v", err)
	}
}

func TestScalarWaitersReleaseOnReinitialize(t *testing.T) {
	streamBuffer, packetBuffer := buf.New(), buf.NewPacket()
	stream := &syscallReadWaiter{buffer: streamBuffer}
	packet := &syscallPacketReadWaiter{buffer: packetBuffer}
	stream.InitializeReadWaiter(N.ReadWaitOptions{})
	packet.InitializeReadWaiter(N.ReadWaitOptions{})
	defer stream.ReleaseReadWaiter()
	defer packet.ReleaseReadWaiter()
	if streamBuffer.RawCap() != 0 || packetBuffer.RawCap() != 0 {
		t.Fatal("waiter initialization retained its old buffer")
	}
}

func TestVectorReadWaiterBoundsBatchAndRejectsUnsupportedReader(t *testing.T) {
	if waiter, ok := CreateVectorisedReadWaiter(bytes.NewReader(nil)); ok || waiter != nil {
		t.Fatal("unsupported reader returned a vector waiter")
	}
	waiter := &vectorisedSyscallReadWaiter{}
	defer waiter.ReleaseReadWaiter()
	for _, size := range []int{0, -1, 1 << 30} {
		waiter.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: size, MaxBufferSize: 128})
		if len(waiter.buffers) < 1 || len(waiter.buffers) > MaxReadBatchSize {
			t.Fatalf("unbounded descriptor allocation for batch size %d", size)
		}
	}
}

type protocolPolicyReader struct{ io.Reader }

func (*protocolPolicyReader) ReaderReplaceable() bool { return false }
func (r *protocolPolicyReader) Upstream() any         { return r.Reader }
func (r *protocolPolicyReader) ReadBuffer(buffer *buf.Buffer) error {
	n, err := r.Reader.Read(buffer.FreeBytes())
	buffer.Truncate(n)
	for index := range buffer.Bytes() {
		buffer.Bytes()[index] ^= 0xff
	}
	if n > 0 && errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func TestCopyPolicyDoesNotBypassProtocolReader(t *testing.T) {
	client, source := TCPPipe(t)
	source.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := client.Write([]byte{0xfe, 0xfd, 0xfc}); err != nil {
		t.Fatal(err)
	}
	client.(*net.TCPConn).CloseWrite()
	var output bytes.Buffer
	n, err := CopyWithOptions(&output, &protocolPolicyReader{source}, CopyOptions{ReadPolicy: func() CopyReadOptions {
		return CopyReadOptions{BatchSize: 4, MaxBufferSize: 128}
	}})
	if err != nil || n != 3 || !bytes.Equal(output.Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("protocol reader was bypassed: n=%d data=%x err=%v", n, output.Bytes(), err)
	}
}

type handshakePolicyReader struct {
	io.Reader
	cached   *buf.Buffer
	failures int
}

func (r *handshakePolicyReader) ReadCached() *buf.Buffer {
	buffer := r.cached
	r.cached = nil
	return buffer
}
func (r *handshakePolicyReader) HandshakeFailure(error) error { r.failures++; return nil }

type secondWriteFailure struct{ calls int }

func (w *secondWriteFailure) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return len(p), nil
	}
	return 0, io.ErrClosedPipe
}

func TestCopyPolicyDoesNotReportHandshakeFailureAfterCachedData(t *testing.T) {
	prefix := buf.NewSize(3)
	prefix.WriteString("pre")
	reader := &handshakePolicyReader{Reader: bytes.NewBufferString("body"), cached: prefix}
	var counted int64
	n, err := CopyWithOptions(&secondWriteFailure{}, reader, CopyOptions{
		BeforeTransfer: func(int64) error { return nil },
		WriteCounters:  []N.CountFunc{func(size int64) { counted += size }},
	})
	if n != 3 || counted != 3 || !errors.Is(err, io.ErrClosedPipe) || reader.failures != 0 {
		t.Fatalf("post-prefix failure: n=%d counted=%d handshakes=%d err=%v", n, counted, reader.failures, err)
	}
}

type possiblePolicyWriter struct {
	policyTestWriter
	checks int
}

func (w *possiblePolicyWriter) WriterPossiblyReplaceable() bool { w.checks++; return true }
func (*possiblePolicyWriter) WriterReplaceable() bool           { return false }

func TestCopyPolicyBoundsHandshakeProbing(t *testing.T) {
	payload := bytes.Repeat([]byte{42}, 5000)
	reader := &policyTestReader{data: bytes.NewReader(payload)}
	writer := &possiblePolicyWriter{}
	n, err := CopyWithOptions(writer, reader, CopyOptions{ReadPolicy: func() CopyReadOptions {
		return CopyReadOptions{MaxBufferSize: 128}
	}})
	if err != nil || n != int64(len(payload)) || !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("handshake transition lost data: n=%d err=%v", n, err)
	}
	if writer.checks != MaxCopyExtendedOnceTimes {
		t.Fatalf("handshake probes=%d, want %d", writer.checks, MaxCopyExtendedOnceTimes)
	}
	assertPolicyBuffersReleased(t, reader)
}
