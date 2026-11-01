package bufio

import "testing"

func TestResolveTransferSize(t *testing.T) {
	if resolveTransferSize(nil) != maxSpliceSize {
		t.Fatal("default cap")
	}
	for _, size := range []int{-1, 0, 1, 4096, maxSpliceSize, maxSpliceSize + 1} {
		want := size
		if want <= 0 || want > maxSpliceSize {
			want = maxSpliceSize
		}
		if got := resolveTransferSize(func() int { return size }); got != want {
			t.Fatalf("size=%d got=%d want=%d", size, got, want)
		}
	}
}
