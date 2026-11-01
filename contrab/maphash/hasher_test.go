package maphash

import (
	"net/netip"
	"testing"
)

func TestHasherEqualCompositeKeys(t *testing.T) {
	type key struct {
		Address netip.AddrPort
		Name    string
		Number  int
	}
	h := NewHasher[key]()
	a := key{Address: netip.MustParseAddrPort("127.0.0.1:1234"), Name: "same", Number: 7}
	b := key{Address: netip.MustParseAddrPort("127.0.0.1:1234"), Name: string([]byte{'s', 'a', 'm', 'e'}), Number: 7}
	for index := 0; index < 100; index++ {
		if h.Hash(a) != h.Hash(b) || h.Hash32(a) != uint32(h.Hash(a)) {
			t.Fatal("equal keys changed shards")
		}
	}
	other := NewSeed(h)
	if other.Hash(a) != other.Hash(b) {
		t.Fatal("new seed did not preserve equality")
	}
}
