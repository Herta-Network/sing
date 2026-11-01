package replay

import (
	"crypto/sha256"
	"sync"
	"time"
)

const defaultMaxEntries = 256 * 1024

// WindowOptions bounds replay state without evicting still-valid salts.
type WindowOptions struct {
	Timeout    time.Duration
	MaxEntries int
	TimeFunc   func() time.Time
}

// WindowFilter uses two rotating windows and fixed-size keys. A window lasts
// Timeout, and entry offsets preserve the full timeout even for salts inserted
// at the end of a window. Each window reserves at most half the total capacity;
// saturation rejects new salts rather than forgetting accepted ones.
type WindowFilter struct {
	access     sync.Mutex
	now        func() time.Time
	timeout    time.Duration
	maxEntries int
	maxCurrent int
	rotatedAt  time.Time
	previousAt time.Time
	current    map[saltKey]time.Duration
	previous   map[saltKey]time.Duration
}

// SimpleFilter remains available for callers referring to the original type.
type SimpleFilter = WindowFilter

type saltKey struct {
	value  [sha256.Size]byte
	length uint8
}

func NewSimple(timeout time.Duration) Filter {
	return NewWindow(WindowOptions{Timeout: timeout, MaxEntries: defaultMaxEntries})
}

func NewWindow(options WindowOptions) *WindowFilter {
	if options.Timeout <= 0 {
		options.Timeout = time.Minute
	}
	if options.MaxEntries <= 0 {
		options.MaxEntries = defaultMaxEntries
	}
	if options.TimeFunc == nil {
		options.TimeFunc = time.Now
	}
	return &WindowFilter{
		now: options.TimeFunc, timeout: options.Timeout,
		maxEntries: options.MaxEntries, maxCurrent: options.MaxEntries/2 + options.MaxEntries%2,
		rotatedAt: options.TimeFunc(), current: make(map[saltKey]time.Duration),
		previous: make(map[saltKey]time.Duration),
	}
}

func (f *WindowFilter) Check(salt []byte) bool {
	key := makeSaltKey(salt)
	f.access.Lock()
	defer f.access.Unlock()
	now := f.now()
	f.rotate(now)
	if f.contains(f.current, f.rotatedAt, key, now) || f.contains(f.previous, f.previousAt, key, now) {
		return false
	}
	if len(f.current) >= f.maxCurrent || len(f.current)+len(f.previous) >= f.maxEntries {
		return false
	}
	f.current[key] = now.Sub(f.rotatedAt)
	return true
}

func (f *WindowFilter) contains(window map[saltKey]time.Duration, start time.Time, key saltKey, now time.Time) bool {
	if offset, found := window[key]; found {
		if now.Sub(start.Add(offset)) < f.timeout {
			return true
		}
		delete(window, key)
	}
	return false
}

func (f *WindowFilter) Clear() {
	f.access.Lock()
	defer f.access.Unlock()
	f.current, f.previous = make(map[saltKey]time.Duration), make(map[saltKey]time.Duration)
	f.rotatedAt, f.previousAt = f.now(), time.Time{}
}

// Len includes expired entries that have not yet been queried or rotated out.
func (f *WindowFilter) Len() int {
	f.access.Lock()
	defer f.access.Unlock()
	f.rotate(f.now())
	return len(f.current) + len(f.previous)
}

func (f *WindowFilter) rotate(now time.Time) {
	elapsed := now.Sub(f.rotatedAt)
	if elapsed < f.timeout {
		return
	}
	if elapsed/f.timeout >= 2 {
		f.previous = make(map[saltKey]time.Duration)
		f.previousAt = time.Time{}
	} else {
		f.previous, f.previousAt = f.current, f.rotatedAt
	}
	f.current = make(map[saltKey]time.Duration)
	f.rotatedAt = now
}

func makeSaltKey(salt []byte) saltKey {
	var key saltKey
	if len(salt) <= len(key.value) {
		copy(key.value[:], salt)
		key.length = uint8(len(salt))
	} else {
		key.value = sha256.Sum256(salt)
		key.length = ^uint8(0)
	}
	return key
}
