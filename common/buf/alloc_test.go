package buf

import "testing"

func TestLargePacketWithHeadroomRemainsPoolManaged(t *testing.T) {
	for _, size := range []int{65535, 65536, 65535 + 1024 + 16, MaxPooledBufferSize} {
		buffer := NewSize(size)
		if buffer.FreeLen() != size || !buffer.managed {
			t.Fatalf("size=%d did not use the frame pool", size)
		}
		buffer.Extend(size)
		buffer.Release()
		if buffer.RawCap() != 0 {
			t.Fatalf("size=%d retained pool storage after release", size)
		}
	}
}

func TestAllocatorRejectsInvalidCapacities(t *testing.T) {
	allocator := newDefaultAllocator()
	for _, size := range []int{0, 1, 65, 65535, 65537, MaxPooledBufferSize + 1} {
		if err := allocator.Put(make([]byte, size)); err == nil {
			t.Fatalf("accepted non-pool capacity %d", size)
		}
	}
	for _, size := range []int{1, 64, 65, 65536, 65537, MaxPooledBufferSize} {
		data := allocator.Get(size)
		if len(data) != size {
			t.Fatalf("size=%d got=%d", size, len(data))
		}
		if err := allocator.Put(data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReleaseDoesNotRecycleBorrowedData(t *testing.T) {
	data := []byte("borrowed")
	buffer := As(data)
	buffer.Release()
	if string(data) != "borrowed" || string(buffer.Bytes()) != "borrowed" {
		t.Fatal("release altered borrowed storage")
	}
}
