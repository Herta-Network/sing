//go:build go1.24

package maphash

import "hash/maphash"

// Hasher hashes comparable keys with a per-instance seed. Modern Go exposes
// this operation directly, avoiding a dependency on runtime map layouts.
type Hasher[K comparable] struct {
	seed maphash.Seed
}

func NewHasher[K comparable]() Hasher[K]        { return Hasher[K]{seed: maphash.MakeSeed()} }
func NewSeed[K comparable](Hasher[K]) Hasher[K] { return NewHasher[K]() }
func (h Hasher[K]) Hash(key K) uint64           { return maphash.Comparable(h.seed, key) }
func (h Hasher[K]) Hash32(key K) uint32         { return uint32(h.Hash(key)) }
