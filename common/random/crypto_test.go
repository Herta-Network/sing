package random

import (
	"errors"
	"math"
	"testing"
)

func TestCryptoIntNRange(t *testing.T) {
	for _, bound := range []int{1, 2, 3, 6, 43, 256, 500, 1<<16 + 1} {
		for index := 0; index < 100; index++ {
			value, err := CryptoIntN(bound)
			if err != nil {
				t.Fatal(err)
			}
			if value < 0 || value >= bound {
				t.Fatalf("CryptoIntN(%d) = %d", bound, value)
			}
		}
	}
}

func TestCryptoIntNRejectsInvalidBound(t *testing.T) {
	if _, err := CryptoIntN(0); !errors.Is(err, ErrInvalidBound) {
		t.Fatalf("error = %v", err)
	}
}

func TestCryptoUint64NBoundaryBounds(t *testing.T) {
	if _, err := CryptoUint64N(0); !errors.Is(err, ErrInvalidBound) {
		t.Fatal("zero bound was accepted")
	}
	for _, bound := range []uint64{1, 2, 1 << 63, 1<<63 + 1, math.MaxUint64} {
		for index := 0; index < 100; index++ {
			value, err := CryptoUint64N(bound)
			if err != nil || value >= bound {
				t.Fatalf("bound=%d value=%d err=%v", bound, value, err)
			}
		}
	}
}

func BenchmarkCryptoIntN(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := CryptoIntN(500); err != nil {
			b.Fatal(err)
		}
	}
}
