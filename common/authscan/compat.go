package authscan

// Local equivalents keep the vendored policy code compatible with Go 1.20.
type ordered interface{ ~int | ~int64 | ~float64 }

func min[T ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
