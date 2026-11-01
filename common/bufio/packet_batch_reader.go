//go:build linux || netbsd || darwin

package bufio

// Only buffers still owned by the waiter are released. Scratch result slices
// may refer to buffers already transferred to the caller and are only cleared.
func (w *syscallPacketBatchReadWaiter) releaseBuffers() {
	for index, buffer := range w.buffers {
		buffer.Release()
		w.buffers[index] = nil
	}
	clearPacketSlice(w.readBuffers)
	clearPacketSlice(w.readDests)
	clearPacketSlice(w.destinations)
	w.clearReadVectors()
	w.readN = 0
}

func (w *syscallPacketBatchReadWaiter) clearReadVectors() {
	clearPacketSlice(w.iovecs)
	clearPacketSlice(w.msgvec)
	clearPacketSlice(w.names)
	clearPacketSlice(w.overflow)
}

func (w *syscallPacketBatchReadWaiter) ReleaseReadWaiter() {
	w.releaseBuffers()
	w.buffers, w.readBuffers = nil, nil
	w.destinations, w.readDests = nil, nil
	w.names, w.iovecs, w.msgvec = nil, nil, nil
	w.overflow = nil
	w.readFunc, w.readErr = nil, nil
}
