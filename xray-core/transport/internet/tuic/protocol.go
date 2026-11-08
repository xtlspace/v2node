package tuic

import (
	"encoding/binary"
	"io"
	stdnet "net"

	xnet "github.com/xtls/xray-core/common/net"
)

const (
	tuicVersion byte = 5

	commandAuthenticate byte = 0
	commandConnect      byte = 1
	commandPacket       byte = 2
	commandDissociate   byte = 3
	commandHeartbeat    byte = 4
)

const (
	TUICVersion byte = tuicVersion

	TUICCommandAuthenticate byte = commandAuthenticate
	TUICCommandConnect      byte = commandConnect
	TUICCommandPacket       byte = commandPacket
	TUICCommandDissociate   byte = commandDissociate
	TUICCommandHeartbeat    byte = commandHeartbeat
)

const authenticateLen = 2 + 16 + 32

const (
	addressTypeDomain byte = 0x00
	addressTypeIPv4   byte = 0x01
	addressTypeIPv6   byte = 0x02
	addressTypeNone   byte = 0xff
)

func ReadDestination(reader io.Reader, network xnet.Network) (xnet.Destination, error) {
	return readDestination(reader, network)
}

func readDestination(reader io.Reader, network xnet.Network) (xnet.Destination, error) {
	var addressType [1]byte
	if _, err := io.ReadFull(reader, addressType[:]); err != nil {
		return xnet.Destination{}, err
	}
	switch addressType[0] {
	case addressTypeNone:
		return xnet.Destination{}, nil
	case addressTypeDomain:
		var domainLength [1]byte
		if _, err := io.ReadFull(reader, domainLength[:]); err != nil {
			return xnet.Destination{}, err
		}
		domain := make([]byte, int(domainLength[0]))
		if _, err := io.ReadFull(reader, domain); err != nil {
			return xnet.Destination{}, err
		}
		port, err := readPort(reader)
		if err != nil {
			return xnet.Destination{}, err
		}
		return destinationFromAddress(network, xnet.DomainAddress(string(domain)), port), nil
	case addressTypeIPv4:
		var ip [stdnet.IPv4len]byte
		if _, err := io.ReadFull(reader, ip[:]); err != nil {
			return xnet.Destination{}, err
		}
		port, err := readPort(reader)
		if err != nil {
			return xnet.Destination{}, err
		}
		return destinationFromAddress(network, xnet.IPAddress(ip[:]), port), nil
	case addressTypeIPv6:
		var ip [stdnet.IPv6len]byte
		if _, err := io.ReadFull(reader, ip[:]); err != nil {
			return xnet.Destination{}, err
		}
		port, err := readPort(reader)
		if err != nil {
			return xnet.Destination{}, err
		}
		return destinationFromAddress(network, xnet.IPAddress(ip[:]), port), nil
	default:
		return xnet.Destination{}, io.ErrUnexpectedEOF
	}
}

// putDestination encodes destination into dst and returns the number of bytes
// written. dst must be at least destinationLen(destination) bytes long.
func putDestination(dst []byte, destination xnet.Destination) (int, error) {
	if !destination.IsValid() {
		if len(dst) < 1 {
			return 0, io.ErrShortBuffer
		}
		dst[0] = addressTypeNone
		return 1, nil
	}
	switch {
	case destination.Address.Family().IsDomain():
		domain := destination.Address.Domain()
		if len(domain) > 255 {
			return 0, io.ErrShortBuffer
		}
		need := 1 + 1 + len(domain) + 2
		if len(dst) < need {
			return 0, io.ErrShortBuffer
		}
		dst[0] = addressTypeDomain
		dst[1] = byte(len(domain))
		copy(dst[2:], domain)
		binary.BigEndian.PutUint16(dst[2+len(domain):need], uint16(destination.Port))
		return need, nil
	case destination.Address.Family().IsIPv4():
		const need = 1 + stdnet.IPv4len + 2
		if len(dst) < need {
			return 0, io.ErrShortBuffer
		}
		ip := destination.Address.IP().To4()
		if ip == nil {
			return 0, io.ErrShortBuffer
		}
		dst[0] = addressTypeIPv4
		copy(dst[1:], ip)
		binary.BigEndian.PutUint16(dst[1+stdnet.IPv4len:need], uint16(destination.Port))
		return need, nil
	case destination.Address.Family().IsIPv6():
		const need = 1 + stdnet.IPv6len + 2
		if len(dst) < need {
			return 0, io.ErrShortBuffer
		}
		ip := destination.Address.IP().To16()
		if ip == nil {
			return 0, io.ErrShortBuffer
		}
		dst[0] = addressTypeIPv6
		copy(dst[1:], ip)
		binary.BigEndian.PutUint16(dst[1+stdnet.IPv6len:need], uint16(destination.Port))
		return need, nil
	default:
		if len(dst) < 1 {
			return 0, io.ErrShortBuffer
		}
		dst[0] = addressTypeNone
		return 1, nil
	}
}

// destinationFromBytes decodes a destination from b and reports how many bytes
// it consumed. The returned address does not retain b.
func destinationFromBytes(b []byte, network xnet.Network) (xnet.Destination, int, error) {
	if len(b) < 1 {
		return xnet.Destination{}, 0, io.ErrUnexpectedEOF
	}
	switch b[0] {
	case addressTypeNone:
		return xnet.Destination{}, 1, nil
	case addressTypeDomain:
		if len(b) < 2 {
			return xnet.Destination{}, 0, io.ErrUnexpectedEOF
		}
		domainLen := int(b[1])
		need := 2 + domainLen + 2
		if len(b) < need {
			return xnet.Destination{}, 0, io.ErrUnexpectedEOF
		}
		port := xnet.Port(binary.BigEndian.Uint16(b[2+domainLen : need]))
		domain := xnet.DomainAddress(string(b[2 : 2+domainLen]))
		return destinationFromAddress(network, domain, port), need, nil
	case addressTypeIPv4:
		const need = 1 + stdnet.IPv4len + 2
		if len(b) < need {
			return xnet.Destination{}, 0, io.ErrUnexpectedEOF
		}
		port := xnet.Port(binary.BigEndian.Uint16(b[1+stdnet.IPv4len : need]))
		return destinationFromAddress(network, xnet.IPAddress(b[1:1+stdnet.IPv4len]), port), need, nil
	case addressTypeIPv6:
		const need = 1 + stdnet.IPv6len + 2
		if len(b) < need {
			return xnet.Destination{}, 0, io.ErrUnexpectedEOF
		}
		port := xnet.Port(binary.BigEndian.Uint16(b[1+stdnet.IPv6len : need]))
		return destinationFromAddress(network, xnet.IPAddress(b[1:1+stdnet.IPv6len]), port), need, nil
	default:
		return xnet.Destination{}, 0, io.ErrUnexpectedEOF
	}
}

func destinationLen(destination xnet.Destination) int {
	if !destination.IsValid() {
		return 1
	}
	switch {
	case destination.Address.Family().IsDomain():
		return 1 + 1 + len(destination.Address.Domain()) + 2
	case destination.Address.Family().IsIPv4():
		return 1 + stdnet.IPv4len + 2
	case destination.Address.Family().IsIPv6():
		return 1 + stdnet.IPv6len + 2
	default:
		return 1
	}
}

func readPort(reader io.Reader) (xnet.Port, error) {
	var b [2]byte
	if _, err := io.ReadFull(reader, b[:]); err != nil {
		return 0, err
	}
	return xnet.Port(binary.BigEndian.Uint16(b[:])), nil
}

func destinationFromAddress(network xnet.Network, address xnet.Address, port xnet.Port) xnet.Destination {
	switch network {
	case xnet.Network_TCP:
		return xnet.TCPDestination(address, port)
	case xnet.Network_UDP:
		return xnet.UDPDestination(address, port)
	default:
		return xnet.Destination{}
	}
}
