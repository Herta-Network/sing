package varbin

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

func TestReadSlice(t *testing.T) {
	var encoded bytes.Buffer
	if _, err := WriteUvarint(&encoded, 3); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&encoded, binary.BigEndian, []uint16{1, 2, 65535}); err != nil {
		t.Fatal(err)
	}
	values, err := ReadSlice[uint16](&encoded, binary.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0] != 1 || values[1] != 2 || values[2] != 65535 {
		t.Fatalf("unexpected values: %v", values)
	}
}

func TestReadSliceHugeTruncatedInputDoesNotPreallocateCount(t *testing.T) {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], math.MaxUint64)
	_, err := ReadSlice[uint64](bytes.NewReader(encoded[:length]), binary.BigEndian)
	if err == nil || err != io.EOF {
		t.Fatalf("ReadSlice() error = %v, want EOF", err)
	}
}
