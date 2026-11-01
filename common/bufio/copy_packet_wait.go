package bufio

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"os"
)

func copyPacketWaitWithPool(session *packetCopySession, destinationConn N.PacketWriter, source N.PacketReadWaiter, notFirstTime bool) (handled bool, n int64, err error) {
	handled = true
	var (
		buffer      *buf.Buffer
		destination M.Socksaddr
	)
	for {
		buffer, destination, err = source.WaitReadPacket()
		if err != nil {
			return
		}
		dataLen := buffer.Len()
		err = destinationConn.WritePacket(buffer, destination)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		n += int64(dataLen)
		if err = session.Transfer(int64(dataLen)); err != nil {
			return
		}
		notFirstTime = true
	}
}

func copyPacketBatchWaitWithPool(session *packetCopySession, destinationConn N.PacketBatchWriter, source N.PacketBatchReadWaiter, notFirstTime bool) (handled bool, n int64, err error) {
	handled = true
	for {
		var (
			buffers      []*buf.Buffer
			destinations []M.Socksaddr
		)
		buffers, destinations, err = source.WaitReadPackets()
		if err != nil {
			buf.ReleaseMulti(buffers)
			return handled, n, err
		}
		if len(buffers) == 0 || len(buffers) != len(destinations) {
			buf.ReleaseMulti(buffers)
			return handled, n, os.ErrInvalid
		}
		var inlineDataLens [DefaultPacketReadBatchSize]int
		dataLens := inlineDataLens[:minPacketInt(len(buffers), len(inlineDataLens))]
		if len(buffers) > len(inlineDataLens) {
			dataLens = make([]int, len(buffers))
		}
		for index, buffer := range buffers {
			dataLens[index] = buffer.Len()
		}
		err = destinationConn.WritePacketBatch(buffers, destinations)
		if err != nil {
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		for _, dataLen := range dataLens {
			n += int64(dataLen)
		}
		if err = session.TransferBatch(dataLens); err != nil {
			return
		}
		notFirstTime = true
	}
}

func copyPacketBatchToConnectedWaitWithPool(session *packetCopySession, destinationConn N.ConnectedPacketBatchWriter, source N.PacketBatchReadWaiter, notFirstTime bool) (handled bool, n int64, err error) {
	handled = true
	for {
		var (
			buffers      []*buf.Buffer
			destinations []M.Socksaddr
		)
		buffers, destinations, err = source.WaitReadPackets()
		if err != nil {
			buf.ReleaseMulti(buffers)
			return handled, n, err
		}
		if len(buffers) == 0 || len(buffers) != len(destinations) {
			buf.ReleaseMulti(buffers)
			return handled, n, os.ErrInvalid
		}
		var inlineDataLens [DefaultPacketReadBatchSize]int
		dataLens := inlineDataLens[:minPacketInt(len(buffers), len(inlineDataLens))]
		if len(buffers) > len(inlineDataLens) {
			dataLens = make([]int, len(buffers))
		}
		for index, buffer := range buffers {
			dataLens[index] = buffer.Len()
		}
		err = destinationConn.WriteConnectedPacketBatch(buffers)
		if err != nil {
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		for _, dataLen := range dataLens {
			n += int64(dataLen)
		}
		if err = session.TransferBatch(dataLens); err != nil {
			return
		}
		notFirstTime = true
	}
}

func copyConnectedPacketBatchWaitWithPool(session *packetCopySession, destinationConn N.PacketBatchWriter, source N.ConnectedPacketBatchReadWaiter, notFirstTime bool) (handled bool, n int64, err error) {
	handled = true
	for {
		var (
			buffers     []*buf.Buffer
			destination M.Socksaddr
		)
		buffers, destination, err = source.WaitReadConnectedPackets()
		if err != nil {
			buf.ReleaseMulti(buffers)
			return handled, n, err
		}
		if len(buffers) == 0 {
			buf.ReleaseMulti(buffers)
			return handled, n, os.ErrInvalid
		}
		var inlineDestinations [DefaultPacketReadBatchSize]M.Socksaddr
		destinations := inlineDestinations[:minPacketInt(len(buffers), len(inlineDestinations))]
		if len(buffers) > len(inlineDestinations) {
			destinations = make([]M.Socksaddr, len(buffers))
		}
		var inlineDataLens [DefaultPacketReadBatchSize]int
		dataLens := inlineDataLens[:minPacketInt(len(buffers), len(inlineDataLens))]
		if len(buffers) > len(inlineDataLens) {
			dataLens = make([]int, len(buffers))
		}
		for index, buffer := range buffers {
			destinations[index] = destination
			dataLens[index] = buffer.Len()
		}
		err = destinationConn.WritePacketBatch(buffers, destinations)
		if err != nil {
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		for _, dataLen := range dataLens {
			n += int64(dataLen)
		}
		if err = session.TransferBatch(dataLens); err != nil {
			return
		}
		notFirstTime = true
	}
}

func copyConnectedPacketBatchToConnectedWaitWithPool(session *packetCopySession, destinationConn N.ConnectedPacketBatchWriter, source N.ConnectedPacketBatchReadWaiter, notFirstTime bool) (handled bool, n int64, err error) {
	handled = true
	for {
		var buffers []*buf.Buffer
		buffers, _, err = source.WaitReadConnectedPackets()
		if err != nil {
			buf.ReleaseMulti(buffers)
			return handled, n, err
		}
		if len(buffers) == 0 {
			buf.ReleaseMulti(buffers)
			return handled, n, os.ErrInvalid
		}
		var inlineDataLens [DefaultPacketReadBatchSize]int
		dataLens := inlineDataLens[:minPacketInt(len(buffers), len(inlineDataLens))]
		if len(buffers) > len(inlineDataLens) {
			dataLens = make([]int, len(buffers))
		}
		for index, buffer := range buffers {
			dataLens[index] = buffer.Len()
		}
		err = destinationConn.WriteConnectedPacketBatch(buffers)
		if err != nil {
			if !notFirstTime {
				handshakeErr := N.ReportHandshakeFailure(session.originSource, err)
				if handshakeErr != nil {
					err = handshakeErr
				}
			}
			return
		}
		for _, dataLen := range dataLens {
			n += int64(dataLen)
		}
		if err = session.TransferBatch(dataLens); err != nil {
			return
		}
		notFirstTime = true
	}
}
