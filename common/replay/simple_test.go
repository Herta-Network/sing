package replay

import (
	"bytes"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWindowFilterRejectsReplayAndRotates(t *testing.T) {
	now := time.Unix(100, 0)
	f := NewWindow(WindowOptions{Timeout: 10 * time.Second, MaxEntries: 8, TimeFunc: func() time.Time { return now }})
	if !f.Check([]byte("salt")) || f.Check([]byte("salt")) {
		t.Fatal("replay was not rejected")
	}
	now = now.Add(5 * time.Second)
	if f.Check([]byte("salt")) {
		t.Fatal("salt expired early")
	}
	now = now.Add(5 * time.Second)
	if !f.Check([]byte("salt")) {
		t.Fatal("expired salt was rejected")
	}
}

func TestWindowFilterKeepsLateSaltForFullTimeout(t *testing.T) {
	now := time.Unix(100, 0)
	f := NewWindow(WindowOptions{Timeout: 10 * time.Second, MaxEntries: 8, TimeFunc: func() time.Time { return now }})
	now = now.Add(4 * time.Second)
	if !f.Check([]byte("late")) {
		t.Fatal("first salt was rejected")
	}
	now = now.Add(time.Second)
	f.Check([]byte("rotate"))
	now = now.Add(5 * time.Second)
	if f.Check([]byte("late")) {
		t.Fatal("window rotation forgot a salt before its full timeout")
	}
	now = now.Add(4 * time.Second)
	if !f.Check([]byte("late")) {
		t.Fatal("salt remained active after its full timeout")
	}
}

func TestWindowFilterBoundsBothWindows(t *testing.T) {
	for _, limit := range []int{1, 3, 8, 31} {
		now := time.Unix(100, 0)
		f := NewWindow(WindowOptions{Timeout: time.Minute, MaxEntries: limit, TimeFunc: func() time.Time { return now }})
		for epoch := 0; epoch < 8; epoch++ {
			for value := 0; value < 128; value++ {
				f.Check([]byte{byte(epoch), byte(value)})
				if f.Len() > limit {
					t.Fatalf("limit=%d retained=%d", limit, f.Len())
				}
			}
			now = now.Add(40 * time.Second)
		}
		f.Clear()
		if f.Len() != 0 {
			t.Fatal("Clear retained replay entries")
		}
	}
}

func TestWindowFilterSaturationDoesNotForgetAcceptedSalt(t *testing.T) {
	f := NewWindow(WindowOptions{MaxEntries: 4, Timeout: time.Minute})
	if !f.Check([]byte("first")) {
		t.Fatal("first salt rejected")
	}
	for value := 0; value < 100; value++ {
		f.Check([]byte{byte(value)})
	}
	if f.Check([]byte("first")) {
		t.Fatal("capacity pressure evicted a live salt")
	}
}

func TestWindowFilterSaltEncodingAndOwnership(t *testing.T) {
	f := NewWindow(WindowOptions{MaxEntries: 32, Timeout: time.Minute})
	long := bytes.Repeat([]byte{42}, 128)
	digest := sha256.Sum256(long)
	values := [][]byte{nil, {0}, {0, 0}, long, digest[:]}
	for _, value := range values {
		if !f.Check(value) || f.Check(value) {
			t.Fatal("salt encoding collided or failed replay detection")
		}
	}
	mutable := []byte("mutable")
	f.Check(mutable)
	mutable[0] = 'x'
	if f.Check([]byte("mutable")) {
		t.Fatal("filter retained caller-owned storage")
	}
}

func TestWindowFilterConcurrentDuplicate(t *testing.T) {
	f := NewWindow(WindowOptions{MaxEntries: 8, Timeout: time.Minute})
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for index := 0; index < 64; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if f.Check([]byte("same")) {
				accepted.Add(1)
			}
		}()
	}
	workers.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted duplicate %d times", accepted.Load())
	}
}
