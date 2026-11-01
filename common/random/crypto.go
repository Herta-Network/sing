package random

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
)

var ErrInvalidBound = errors.New("random: bound must be positive")

// CryptoIntN returns an unbiased cryptographically random integer in [0, max).
// It uses fixed stack storage instead of crypto/rand.Int and big.Int.
func CryptoIntN(max int) (int, error) {
	if max <= 0 {
		return 0, ErrInvalidBound
	}
	value, err := CryptoUint64N(uint64(max))
	return int(value), err
}

// CryptoUint64N returns an unbiased cryptographically random integer in [0, max).
func CryptoUint64N(max uint64) (uint64, error) {
	if max == 0 {
		return 0, ErrInvalidBound
	}
	bound := max
	threshold := -bound % bound
	var storage [8]byte
	for {
		if _, err := rand.Read(storage[:]); err != nil {
			return 0, err
		}
		value := binary.LittleEndian.Uint64(storage[:])
		if value >= threshold {
			return value % bound, nil
		}
	}
}
