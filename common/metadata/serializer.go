package metadata

import (
	"encoding/binary"
	"io"
	"net/netip"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
)

const (
	MaxSocksaddrLength   = 2 + 255 + 2
	MaxIPSocksaddrLength = 1 + 16 + 2
)

type SerializerOption func(*Serializer)

func AddressFamilyByte(b byte, f Family) SerializerOption {
	return func(s *Serializer) {
		s.familyMap[b] = f
		s.familyMapSet[b] = true
		s.familyByteMap[f] = b
		s.familyByteSet[f] = true
	}
}

func PortThenAddress() SerializerOption {
	return func(s *Serializer) {
		s.portFirst = true
	}
}

type Serializer struct {
	familyMap     [256]Family
	familyMapSet  [256]bool
	familyByteMap [256]byte
	familyByteSet [256]bool
	portFirst     bool
}

func NewSerializer(options ...SerializerOption) *Serializer {
	s := new(Serializer)
	for _, option := range options {
		option(s)
	}
	return s
}

func (s *Serializer) WriteAddress(buffer *buf.Buffer, addr Socksaddr) error {
	var af Family
	if !addr.IsValid() {
		af = AddressFamilyEmpty
	} else if addr.IsIPv4() {
		af = AddressFamilyIPv4
	} else if addr.IsIPv6() {
		af = AddressFamilyIPv6
	} else {
		af = AddressFamilyFqdn
	}
	afByte := s.familyByteMap[af]
	if !s.familyByteSet[af] {
		return E.New("unsupported address")
	}
	err := buffer.WriteByte(afByte)
	if err != nil {
		return err
	}
	switch af {
	case AddressFamilyIPv4, AddressFamilyIPv6:
		_, err = buffer.Write(addr.Addr.AsSlice())
	case AddressFamilyFqdn:
		err = WriteSocksString(buffer, addr.Fqdn)
	}
	return err
}

func (s *Serializer) AddressLen(addr Socksaddr) int {
	if !addr.IsValid() {
		return 1
	} else if addr.IsIPv4() {
		return 5
	} else if addr.IsIPv6() {
		return 17
	} else {
		return 2 + len(addr.Fqdn)
	}
}

func (s *Serializer) WritePort(writer io.Writer, port uint16) error {
	if buffer, isBuffer := writer.(*buf.Buffer); isBuffer {
		return writePortBuffer(buffer, port)
	}
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], port)
	n, err := writer.Write(encoded[:])
	if err == nil && n != len(encoded) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *Serializer) WriteAddrPort(writer io.Writer, destination Socksaddr) error {
	buffer, isBuffer := writer.(*buf.Buffer)
	if isBuffer {
		return s.WriteAddrPortBuffer(buffer, destination)
	}
	buffer = buf.NewSize(s.AddrPortLen(destination))
	defer buffer.Release()
	err := s.WriteAddrPortBuffer(buffer, destination)
	if err != nil {
		return err
	}
	n, err := writer.Write(buffer.Bytes())
	if err == nil && n != buffer.Len() {
		err = io.ErrShortWrite
	}
	return err
}

func (s *Serializer) WriteAddrPortBuffer(buffer *buf.Buffer, destination Socksaddr) error {
	var err error
	if !s.portFirst {
		err = s.WriteAddress(buffer, destination)
	} else {
		err = writePortBuffer(buffer, destination.Port)
	}
	if err != nil {
		return err
	}
	if s.portFirst {
		err = s.WriteAddress(buffer, destination)
	} else if destination.IsValid() {
		err = writePortBuffer(buffer, destination.Port)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Serializer) AddrPortLen(destination Socksaddr) int {
	if s.portFirst || destination.IsValid() {
		return s.AddressLen(destination) + 2
	} else {
		return s.AddressLen(destination)
	}
}

func (s *Serializer) ReadAddress(reader io.Reader) (Socksaddr, error) {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		return s.readAddressBuffer(buffer)
	}
	af, err := readByte(reader)
	if err != nil {
		return Socksaddr{}, err
	}
	if !s.familyMapSet[af] {
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
	family := s.familyMap[af]
	switch family {
	case AddressFamilyFqdn:
		fqdn, err := ReadSockString(reader)
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read fqdn")
		}
		return ParseSocksaddrHostPort(fqdn, 0), nil
	case AddressFamilyIPv4:
		var addr [4]byte
		err = readFixed(reader, addr[:])
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv4 address")
		}
		return Socksaddr{Addr: netip.AddrFrom4(addr)}, nil
	case AddressFamilyIPv6:
		var addr [16]byte
		err = readFixed(reader, addr[:])
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv6 address")
		}
		return Socksaddr{Addr: netip.AddrFrom16(addr)}.Unwrap(), nil
	case AddressFamilyEmpty:
		return Socksaddr{}, nil
	default:
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
}

func (s *Serializer) readAddressBuffer(reader *buf.Buffer) (Socksaddr, error) {
	af, err := reader.ReadByte()
	if err != nil {
		return Socksaddr{}, err
	}
	if !s.familyMapSet[af] {
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
	switch s.familyMap[af] {
	case AddressFamilyFqdn:
		fqdn, err := readSockStringBuffer(reader)
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read fqdn")
		}
		return ParseSocksaddrHostPort(fqdn, 0), nil
	case AddressFamilyIPv4:
		encoded, err := reader.ReadBytes(4)
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv4 address")
		}
		var addr [4]byte
		copy(addr[:], encoded)
		return Socksaddr{Addr: netip.AddrFrom4(addr)}, nil
	case AddressFamilyIPv6:
		encoded, err := reader.ReadBytes(16)
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv6 address")
		}
		var addr [16]byte
		copy(addr[:], encoded)
		return Socksaddr{Addr: netip.AddrFrom16(addr)}.Unwrap(), nil
	case AddressFamilyEmpty:
		return Socksaddr{}, nil
	default:
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
}

func (s *Serializer) ReadPort(reader io.Reader) (uint16, error) {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		return readPortBuffer(buffer)
	}
	var encoded [2]byte
	err := readFixed(reader, encoded[:])
	if err != nil {
		return 0, E.Cause(err, "read port")
	}
	return binary.BigEndian.Uint16(encoded[:]), nil
}

func (s *Serializer) ReadAddrPort(reader io.Reader) (destination Socksaddr, err error) {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		return s.ReadAddrPortBuffer(buffer)
	}
	var addr Socksaddr
	var port uint16
	if !s.portFirst {
		addr, err = s.ReadAddress(reader)
	} else {
		port, err = s.ReadPort(reader)
	}
	if err != nil {
		return
	}
	if s.portFirst {
		addr, err = s.ReadAddress(reader)
	} else if addr.IsValid() {
		port, err = s.ReadPort(reader)
	}
	if err != nil {
		return
	}
	addr.Port = port
	return addr, nil
}

// ReadAddrPortWithScratch avoids temporary allocations for IP addresses read
// from a stream. Scratch must contain at least 16 bytes and is not retained.
func (s *Serializer) ReadAddrPortWithScratch(reader io.Reader, scratch []byte) (destination Socksaddr, err error) {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		return s.ReadAddrPortBuffer(buffer)
	}
	if len(scratch) < 16 {
		return Socksaddr{}, io.ErrShortBuffer
	}
	var addr Socksaddr
	var port uint16
	if !s.portFirst {
		addr, err = s.readAddressWithScratch(reader, scratch)
	} else {
		port, err = readPortWithScratch(reader, scratch)
	}
	if err != nil {
		return
	}
	if s.portFirst {
		addr, err = s.readAddressWithScratch(reader, scratch)
	} else if addr.IsValid() {
		port, err = readPortWithScratch(reader, scratch)
	}
	if err != nil {
		return
	}
	addr.Port = port
	return addr, nil
}

func (s *Serializer) readAddressWithScratch(reader io.Reader, scratch []byte) (Socksaddr, error) {
	_, err := io.ReadFull(reader, scratch[:1])
	if err != nil {
		return Socksaddr{}, err
	}
	af := scratch[0]
	if !s.familyMapSet[af] {
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
	switch s.familyMap[af] {
	case AddressFamilyFqdn:
		_, err = io.ReadFull(reader, scratch[:1])
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read fqdn")
		}
		fqdnBytes := make([]byte, int(scratch[0]))
		_, err = io.ReadFull(reader, fqdnBytes)
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read fqdn")
		}
		return ParseSocksaddrHostPort(string(fqdnBytes), 0), nil
	case AddressFamilyIPv4:
		_, err = io.ReadFull(reader, scratch[:4])
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv4 address")
		}
		var addr [4]byte
		copy(addr[:], scratch[:4])
		return Socksaddr{Addr: netip.AddrFrom4(addr)}, nil
	case AddressFamilyIPv6:
		_, err = io.ReadFull(reader, scratch[:16])
		if err != nil {
			return Socksaddr{}, E.Cause(err, "read ipv6 address")
		}
		var addr [16]byte
		copy(addr[:], scratch[:16])
		return Socksaddr{Addr: netip.AddrFrom16(addr)}.Unwrap(), nil
	case AddressFamilyEmpty:
		return Socksaddr{}, nil
	default:
		return Socksaddr{}, E.New("unknown address family: ", af)
	}
}

func readPortWithScratch(reader io.Reader, scratch []byte) (uint16, error) {
	_, err := io.ReadFull(reader, scratch[:2])
	if err != nil {
		return 0, E.Cause(err, "read port")
	}
	return binary.BigEndian.Uint16(scratch[:2]), nil
}

func (s *Serializer) ReadAddrPortBuffer(reader *buf.Buffer) (destination Socksaddr, err error) {
	var addr Socksaddr
	var port uint16
	if !s.portFirst {
		addr, err = s.readAddressBuffer(reader)
	} else {
		port, err = readPortBuffer(reader)
	}
	if err != nil {
		return
	}
	if s.portFirst {
		addr, err = s.readAddressBuffer(reader)
	} else if addr.IsValid() {
		port, err = readPortBuffer(reader)
	}
	if err != nil {
		return
	}
	addr.Port = port
	return addr, nil
}

func ReadSockString(reader io.Reader) (string, error) {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		return readSockStringBuffer(buffer)
	}
	strLen, err := readByte(reader)
	if err != nil {
		return "", err
	}
	strBytes := make([]byte, strLen)
	_, err = io.ReadFull(reader, strBytes)
	if err != nil {
		return "", err
	}
	return string(strBytes), nil
}

func readSockStringBuffer(reader *buf.Buffer) (string, error) {
	strLen, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	strBytes, err := reader.ReadBytes(int(strLen))
	if err != nil {
		return "", err
	}
	return string(strBytes), nil
}

func writePortBuffer(buffer *buf.Buffer, port uint16) error {
	if buffer.FreeLen() < 2 {
		return io.ErrShortBuffer
	}
	binary.BigEndian.PutUint16(buffer.Extend(2), port)
	return nil
}

func readPortBuffer(buffer *buf.Buffer) (uint16, error) {
	encoded, err := buffer.ReadBytes(2)
	if err != nil {
		return 0, E.Cause(err, "read port")
	}
	return binary.BigEndian.Uint16(encoded), nil
}

func readByte(reader io.Reader) (byte, error) {
	if byteReader, isByteReader := reader.(io.ByteReader); isByteReader {
		return byteReader.ReadByte()
	}
	var encoded [1]byte
	_, err := io.ReadFull(reader, encoded[:])
	return encoded[0], err
}

func readFixed(reader io.Reader, destination []byte) error {
	if buffer, isBuffer := reader.(*buf.Buffer); isBuffer {
		source, err := buffer.ReadBytes(len(destination))
		if err != nil {
			return err
		}
		copy(destination, source)
		return nil
	}
	_, err := io.ReadFull(reader, destination)
	return err
}

func WriteSocksString(buffer *buf.Buffer, str string) error {
	strLen := len(str)
	if strLen > 255 {
		return E.New("fqdn too long")
	}
	err := buffer.WriteByte(byte(strLen))
	if err != nil {
		return err
	}
	return common.Error(buffer.WriteString(str))
}
