package metadata

import (
	"bytes"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
)

func TestSerializerRoundTrip(t *testing.T) {
	testCases := []Socksaddr{
		ParseSocksaddrHostPort("192.0.2.1", 443),
		ParseSocksaddrHostPort("2001:db8::1", 8443),
		ParseSocksaddrHostPort("example.com", 53),
	}
	serializers := []*Serializer{
		SocksaddrSerializer,
		NewSerializer(
			AddressFamilyByte(0x01, AddressFamilyIPv4),
			AddressFamilyByte(0x04, AddressFamilyIPv6),
			AddressFamilyByte(0x03, AddressFamilyFqdn),
			PortThenAddress(),
		),
	}
	for _, serializer := range serializers {
		for _, destination := range testCases {
			encoded := buf.NewSize(serializer.AddrPortLen(destination))
			if err := serializer.WriteAddrPortBuffer(encoded, destination); err != nil {
				t.Fatal(err)
			}
			decoded, err := serializer.ReadAddrPortBuffer(encoded)
			encoded.Release()
			if err != nil {
				t.Fatal(err)
			}
			if decoded != destination {
				t.Fatalf("decoded address = %v, want %v", decoded, destination)
			}
		}
	}
}

func TestSerializerGenericReaderWriter(t *testing.T) {
	destination := ParseSocksaddrHostPort("198.51.100.5", 5353)
	var encoded bytes.Buffer
	if err := SocksaddrSerializer.WriteAddrPort(&encoded, destination); err != nil {
		t.Fatal(err)
	}
	decoded, err := SocksaddrSerializer.ReadAddrPort(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if decoded != destination {
		t.Fatalf("decoded address = %v, want %v", decoded, destination)
	}
	var scratch [16]byte
	decoded, err = SocksaddrSerializer.ReadAddrPortWithScratch(bytes.NewReader(encoded.Bytes()), scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded != destination {
		t.Fatalf("scratch decoded address = %v, want %v", decoded, destination)
	}
	if _, err = SocksaddrSerializer.ReadAddrPortWithScratch(bytes.NewReader(encoded.Bytes()), scratch[:15]); err != io.ErrShortBuffer {
		t.Fatalf("short scratch error = %v, want %v", err, io.ErrShortBuffer)
	}
}

func TestSerializerScratchRoundTrip(t *testing.T) {
	serializers := []*Serializer{
		SocksaddrSerializer,
		NewSerializer(
			AddressFamilyByte(0x01, AddressFamilyIPv4),
			AddressFamilyByte(0x04, AddressFamilyIPv6),
			AddressFamilyByte(0x03, AddressFamilyFqdn),
			PortThenAddress(),
		),
	}
	destinations := []Socksaddr{
		ParseSocksaddrHostPort("192.0.2.2", 80),
		ParseSocksaddrHostPort("2001:db8::2", 443),
		ParseSocksaddrHostPort("scratch.example", 53),
	}
	var scratch [16]byte
	for _, serializer := range serializers {
		for _, destination := range destinations {
			encoded := buf.NewSize(serializer.AddrPortLen(destination))
			if err := serializer.WriteAddrPortBuffer(encoded, destination); err != nil {
				t.Fatal(err)
			}
			decoded, err := serializer.ReadAddrPortWithScratch(bytes.NewReader(encoded.Bytes()), scratch[:])
			encoded.Release()
			if err != nil {
				t.Fatal(err)
			}
			if decoded != destination {
				t.Fatalf("decoded address = %v, want %v", decoded, destination)
			}
		}
	}
}

func TestSerializerRejectsUnknownAndShortWrites(t *testing.T) {
	if _, err := SocksaddrSerializer.ReadAddress(bytes.NewReader([]byte{0xfe})); err == nil {
		t.Fatal("unknown address family was accepted")
	}
	if err := SocksaddrSerializer.WritePort(shortWriter{}, 443); err != io.ErrShortWrite {
		t.Fatalf("short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if err := SocksaddrSerializer.WriteAddrPort(shortWriter{}, ParseSocksaddrHostPort("example.com", 443)); err != io.ErrShortWrite {
		t.Fatalf("short address write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestSerializerEmptyFamilyAndPortFirst(t *testing.T) {
	for _, portFirst := range []bool{false, true} {
		options := []SerializerOption{AddressFamilyByte(0, AddressFamilyEmpty)}
		if portFirst {
			options = append(options, PortThenAddress())
		}
		serializer := NewSerializer(options...)
		address := Socksaddr{}
		encoded := buf.NewSize(serializer.AddrPortLen(address))
		if err := serializer.WriteAddrPortBuffer(encoded, address); err != nil {
			t.Fatal(err)
		}
		wire := append([]byte(nil), encoded.Bytes()...)
		decoded, err := serializer.ReadAddrPortBuffer(encoded)
		encoded.Release()
		if err != nil || decoded != address {
			t.Fatalf("empty address round trip = %v, %v", decoded, err)
		}
		var scratch [16]byte
		decoded, err = serializer.ReadAddrPortWithScratch(bytes.NewReader(wire), scratch[:])
		if err != nil || decoded != address {
			t.Fatalf("empty scratch round trip = %v, %v", decoded, err)
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) {
	return 1, nil
}

func BenchmarkSerializerIPv4Buffer(b *testing.B) {
	destination := ParseSocksaddrHostPort("192.0.2.1", 443)
	encoded := buf.NewSize(SocksaddrSerializer.AddrPortLen(destination))
	if err := SocksaddrSerializer.WriteAddrPortBuffer(encoded, destination); err != nil {
		b.Fatal(err)
	}
	data := bytes.Clone(encoded.Bytes())
	encoded.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reader := buf.As(data)
		decoded, err := SocksaddrSerializer.ReadAddrPortBuffer(reader)
		if err != nil || decoded != destination {
			b.Fatal(decoded, err)
		}
	}
}
