package metadata

import (
	"bytes"
	"strings"
	"testing"
)

func TestScratchDomainDoesNotAliasReusableStorage(t *testing.T) {
	for _, length := range []int{16, 256} {
		scratch := make([]byte, length)
		for _, name := range []string{"short.example", strings.Repeat("a.", 126) + "a"} {
			var wire bytes.Buffer
			target := ParseSocksaddrHostPort(name, 443)
			if err := SocksaddrSerializer.WriteAddrPort(&wire, target); err != nil {
				t.Fatal(err)
			}
			got, err := SocksaddrSerializer.ReadAddrPortWithScratch(&wire, scratch)
			if err != nil {
				t.Fatal(err)
			}
			for i := range scratch {
				scratch[i] = 0
			}
			if got != target {
				t.Fatal("domain retained reusable scratch")
			}
		}
	}
}

func BenchmarkSerializerDomainScratch(b *testing.B) {
	target := ParseSocksaddrHostPort("service.long-domain.example", 443)
	var wire bytes.Buffer
	if err := SocksaddrSerializer.WriteAddrPort(&wire, target); err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(wire.Bytes())
	var scratch [256]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(wire.Bytes())
		if got, err := SocksaddrSerializer.ReadAddrPortWithScratch(reader, scratch[:]); err != nil || got != target {
			b.Fatal(got, err)
		}
	}
}
