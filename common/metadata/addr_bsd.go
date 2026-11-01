//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package metadata

import (
	"encoding/binary"
	"net/netip"
	"unsafe"

	"golang.org/x/sys/unix"
)

func AddrPortToRawSockaddr(addrPort netip.AddrPort, forceInet6 bool) (name unsafe.Pointer, nameLen uint32) {
	if addrPort.Addr().Is4() && !forceInet6 {
		var sa unix.RawSockaddrInet4
		sa.Len = unix.SizeofSockaddrInet4
		sa.Family = unix.AF_INET
		sa.Addr = addrPort.Addr().As4()
		binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:], addrPort.Port())
		return unsafe.Pointer(&sa), unix.SizeofSockaddrInet4
	} else {
		var sa unix.RawSockaddrInet6
		sa.Len = unix.SizeofSockaddrInet6
		sa.Family = unix.AF_INET6
		sa.Addr = addrPort.Addr().As16()
		binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:], addrPort.Port())
		return unsafe.Pointer(&sa), unix.SizeofSockaddrInet6
	}
}

func AddrPortToRawSockaddrAny(name *unix.RawSockaddrAny, addrPort netip.AddrPort, forceInet6 bool) uint32 {
	*name = unix.RawSockaddrAny{}
	if addrPort.Addr().Is4() && !forceInet6 {
		raw := (*unix.RawSockaddrInet4)(unsafe.Pointer(name))
		raw.Len = unix.SizeofSockaddrInet4
		raw.Family = unix.AF_INET
		raw.Addr = addrPort.Addr().As4()
		binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&raw.Port))[:], addrPort.Port())
		return unix.SizeofSockaddrInet4
	}
	raw := (*unix.RawSockaddrInet6)(unsafe.Pointer(name))
	raw.Len = unix.SizeofSockaddrInet6
	raw.Family = unix.AF_INET6
	raw.Addr = addrPort.Addr().As16()
	binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&raw.Port))[:], addrPort.Port())
	return unix.SizeofSockaddrInet6
}
