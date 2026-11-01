package buf

import "testing"

// Force the same interface escape as an owned writer. The reused descriptor
// case is an experiment: it is valid only when this caller is the sole owner
// and consumption is synchronous, neither of which general packet handoff
// promises. It deliberately does not introduce a public pooling API.
//
//go:noinline
func consumeDescriptor(b *Buffer) { b.Release() }

func BenchmarkBufferDescriptor(b *testing.B) {
	b.Run("owned-transfer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p := NewSize(1500)
			p.Resize(300, 1200)
			consumeDescriptor(p)
		}
	})
	b.Run("exclusive-connection-reuse", func(b *testing.B) {
		p := new(Buffer)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p.data, p.capacity, p.managed = Get(1500), 1500, true
			p.Resize(300, 1200)
			consumeDescriptor(p)
		}
	})
}

func TestReleasedDescriptorDoesNotAliasLaterPacket(t *testing.T) {
	first := NewSize(1500)
	first.Release()
	second := NewSize(1500)
	defer second.Release()
	second.WriteString("later packet")
	// Existing callers can release their consumed descriptor again. Reusing
	// the descriptor would let this release destroy an unrelated active packet.
	first.Release()
	if first.RawCap() != 0 || string(second.Bytes()) != "later packet" {
		t.Fatal("released descriptor aliases a live packet")
	}
}

func TestReferencedDescriptorRetainsStorage(t *testing.T) {
	p := NewSize(1500)
	p.WriteString("retained")
	p.IncRef()
	p.Release()
	if string(p.Bytes()) != "retained" {
		t.Fatal("release ignored outstanding reference")
	}
	p.DecRef()
	p.Release()
	if p.RawCap() != 0 {
		t.Fatal("final release retained storage")
	}
}
