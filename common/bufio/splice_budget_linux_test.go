//go:build linux

package bufio

import "testing"

func TestSpliceSlotLimit(t *testing.T) {
	limit := cap(spliceSlots)
	for index := 0; index < limit; index++ {
		if !tryAcquireSpliceSlot() {
			t.Fatalf("slot %d of %d was unavailable", index+1, limit)
		}
	}
	defer func() {
		for index := 0; index < limit; index++ {
			releaseSpliceSlot()
		}
	}()
	if tryAcquireSpliceSlot() {
		releaseSpliceSlot()
		t.Fatalf("acquired more than %d splice slots", limit)
	}
}

func TestSpliceSaturationDoesNotTouchConnections(t *testing.T) {
	count := cap(spliceSlots)
	for i := 0; i < count; i++ {
		if !tryAcquireSpliceSlot() {
			t.Fatal("unexpected busy slot")
		}
	}
	defer func() {
		for i := 0; i < count; i++ {
			releaseSpliceSlot()
		}
	}()
	// nil RawConns prove fallback happens before even inspecting descriptors.
	handled, n, err := splice(nil, nil, nil, nil, nil, nil)
	if handled || n != 0 || err != nil {
		t.Fatalf("handled=%v n=%d err=%v", handled, n, err)
	}
}
