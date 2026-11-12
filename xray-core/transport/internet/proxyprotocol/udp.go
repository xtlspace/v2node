package proxyprotocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"
)

const (
	// v2HeaderMax is the maximum PROXY protocol v2 header for a UDP datagram:
	// 16 bytes fixed header + 36 bytes IPv6 address block.
	v2HeaderMax = 16 + 36

	entryTTL  = 10 * time.Minute
	sweepOps  = 4096
	sweepTime = time.Minute
)

var (
	v2Signature = []byte{'\r', '\n', '\r', '\n', 0, '\r', '\n', 'Q', 'U', 'I', 'T', '\n'}
	v1Prefix    = []byte("PROXY ")

	errV1Header       = errors.New("PROXY protocol v1 is not supported")
	errInvalidHeader  = errors.New("invalid PROXY protocol v2 header")
	errUnsupportedVer = errors.New("unsupported PROXY protocol version")
)

// Wrap makes a PacketConn understand the PROXY protocol v2 over UDP.
//
// A datagram without a header is accepted as-is (optional mode): the transport
// peer address is used. A malformed header, or a v1 header, is dropped.
func Wrap(pc net.PacketConn) net.PacketConn {
	if udp, ok := pc.(*net.UDPConn); ok {
		return newOOBConn(udp)
	}
	return newBasicConn(pc)
}

type entry struct {
	transport *net.UDPAddr
	logical   *net.UDPAddr
	lastSeen  atomic.Int64
}

type base struct {
	forward sync.Map // netip.AddrPort (transport) -> *entry
	reverse sync.Map // netip.AddrPort (logical) -> *entry
	ops     atomic.Uint64
	sweepAt atomic.Int64
}

func (b *base) init() {
	b.sweepAt.Store(time.Now().UnixNano())
}

func (b *base) maybeSweep() {
	if b.ops.Add(1)%sweepOps != 0 {
		return
	}
	now := time.Now()
	last := b.sweepAt.Load()
	if now.UnixNano()-last < int64(sweepTime) {
		return
	}
	if !b.sweepAt.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	cutoff := now.Add(-entryTTL).UnixNano()
	b.forward.Range(func(key, value any) bool {
		e := value.(*entry)
		if e.lastSeen.Load() < cutoff {
			b.forward.Delete(key)
			if lk, ok := keyFromAddr(e.logical); ok {
				// Only drop the reverse mapping if it still belongs to this
				// entry. The same logical address may have been re-mapped to a
				// newer transport after this entry went stale.
				if cur, ok := b.reverse.Load(lk); ok && cur == e {
					b.reverse.Delete(lk)
				}
			}
		}
		return true
	})
}

func (b *base) lookup(transport net.Addr) *entry {
	k, ok := keyFromAddr(transport)
	if !ok {
		return nil
	}
	if v, ok := b.forward.Load(k); ok {
		e := v.(*entry)
		e.lastSeen.Store(time.Now().UnixNano())
		return e
	}
	return nil
}

func (b *base) record(transport net.Addr, src netip.AddrPort) *net.UDPAddr {
	t, ok := transport.(*net.UDPAddr)
	if !ok {
		return nil
	}
	if !src.IsValid() {
		return b.recordIdentity(t)
	}

	tk, ok := keyFromAddr(t)
	if !ok {
		return t
	}

	now := time.Now().UnixNano()
	if v, ok := b.forward.Load(tk); ok {
		e := v.(*entry)
		if lk, ok := keyFromAddr(e.logical); ok {
			if lk == src {
				e.lastSeen.Store(now)
				b.maybeSweep()
				return e.logical
			}
			b.reverse.Delete(lk)
		}
	}

	logical := addrFromKey(src)
	e := &entry{transport: t, logical: logical}
	e.lastSeen.Store(now)
	b.forward.Store(tk, e)
	b.reverse.Store(src, e)
	b.maybeSweep()
	return logical
}

func (b *base) recordIdentity(t *net.UDPAddr) *net.UDPAddr {
	tk, ok := keyFromAddr(t)
	if !ok {
		return t
	}
	now := time.Now().UnixNano()
	if v, ok := b.forward.Load(tk); ok {
		e := v.(*entry)
		e.lastSeen.Store(now)
		return e.logical
	}
	e := &entry{transport: t, logical: t}
	e.lastSeen.Store(now)
	b.forward.Store(tk, e)
	b.reverse.Store(tk, e)
	b.maybeSweep()
	return t
}

func (b *base) transportFor(addr net.Addr) *net.UDPAddr {
	k, ok := keyFromAddr(addr)
	if !ok {
		return nil
	}
	if v, ok := b.reverse.Load(k); ok {
		e := v.(*entry)
		e.lastSeen.Store(time.Now().UnixNano())
		return e.transport
	}
	return nil
}

func keyFromAddr(addr net.Addr) (netip.AddrPort, bool) {
	ua, ok := addr.(*net.UDPAddr)
	if !ok || ua == nil {
		return netip.AddrPort{}, false
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return netip.AddrPort{}, false
	}
	ip = ip.Unmap()
	if ua.Zone != "" {
		ip = ip.WithZone(ua.Zone)
	}
	return netip.AddrPortFrom(ip, uint16(ua.Port)), true
}

func addrFromKey(k netip.AddrPort) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IP(k.Addr().AsSlice()), Port: int(k.Port())}
}

// parseV2 parses a PROXY protocol v2 header from a UDP datagram.
//
// When hasHeader is false, the datagram did not start with a v2 header and the
// caller should treat the whole datagram as payload. src is invalid for
// LOCAL/UNSPEC headers.
func parseV2(b []byte) (src netip.AddrPort, headerLen int, hasHeader bool, err error) {
	if len(b) < len(v2Signature) {
		if bytes.HasPrefix(b, v1Prefix) {
			return netip.AddrPort{}, 0, false, errV1Header
		}
		return netip.AddrPort{}, 0, false, nil
	}
	if !bytes.Equal(b[:len(v2Signature)], v2Signature) {
		if bytes.HasPrefix(b, v1Prefix) {
			return netip.AddrPort{}, 0, false, errV1Header
		}
		return netip.AddrPort{}, 0, false, nil
	}
	if len(b) < 16 {
		return netip.AddrPort{}, 0, false, errInvalidHeader
	}
	if b[12]>>4 != 2 {
		return netip.AddrPort{}, 0, false, errUnsupportedVer
	}

	length := int(binary.BigEndian.Uint16(b[14:16]))
	headerLen = 16 + length
	if headerLen > len(b) {
		return netip.AddrPort{}, 0, false, errInvalidHeader
	}

	switch b[12] & 0x0f {
	case 0x00: // LOCAL: use the transport address
		return netip.AddrPort{}, headerLen, true, nil
	case 0x01: // PROXY
	default:
		return netip.AddrPort{}, 0, false, errInvalidHeader
	}

	switch b[13] {
	case 0x11, 0x12: // IPv4, stream or datagram
		if length < 12 {
			return netip.AddrPort{}, 0, false, errInvalidHeader
		}
		ip := netip.AddrFrom4([4]byte(b[16:20]))
		port := binary.BigEndian.Uint16(b[24:26])
		return netip.AddrPortFrom(ip, port), headerLen, true, nil

	case 0x21, 0x22: // IPv6, stream or datagram
		if length < 36 {
			return netip.AddrPort{}, 0, false, errInvalidHeader
		}
		ip := netip.AddrFrom16([16]byte(b[16:32])).Unmap()
		port := binary.BigEndian.Uint16(b[48:50])
		return netip.AddrPortFrom(ip, port), headerLen, true, nil

	case 0x00: // UNSPEC
		return netip.AddrPort{}, headerLen, true, nil

	default:
		return netip.AddrPort{}, 0, false, errInvalidHeader
	}
}

type oobConn struct {
	*net.UDPConn
	base
	batch *ipv4.PacketConn
}

func newOOBConn(udp *net.UDPConn) *oobConn {
	c := &oobConn{}
	c.UDPConn = udp
	c.base.init()
	c.batch = ipv4.NewPacketConn(udp)
	return c
}

func (c *oobConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, transport, err := c.UDPConn.ReadFrom(p)
		if err != nil {
			return 0, nil, err
		}
		src, headerLen, hasHeader, perr := parseV2(p[:n])
		if perr != nil {
			continue
		}
		if !hasHeader {
			if e := c.lookup(transport); e != nil {
				return n, e.logical, nil
			}
			if ua, ok := transport.(*net.UDPAddr); ok {
				c.recordIdentity(ua)
			}
			return n, transport, nil
		}

		copy(p, p[headerLen:n])
		n -= headerLen
		if n == 0 {
			continue
		}
		return n, c.record(transport, src), nil
	}
}

func (c *oobConn) ReadMsgUDP(b, oob []byte) (n, oobn, flags int, addr *net.UDPAddr, err error) {
	for {
		n, oobn, flags, addr, err = c.UDPConn.ReadMsgUDP(b, oob)
		if err != nil {
			return
		}
		src, headerLen, hasHeader, perr := parseV2(b[:n])
		if perr != nil {
			continue
		}
		if !hasHeader {
			if e := c.lookup(addr); e != nil {
				addr = e.logical
			} else {
				c.recordIdentity(addr)
			}
			return n, oobn, flags, addr, nil
		}

		copy(b, b[headerLen:n])
		n -= headerLen
		if n == 0 {
			continue
		}
		if logical := c.record(addr, src); logical != nil {
			addr = logical
		}
		return n, oobn, flags, addr, nil
	}
}

// retainMessage moves the message at src to dst by swapping the two slots.
//
// quic-go allocates one OOB (control message) buffer per message slot once at
// startup and never reassigns them. A plain struct copy (ms[dst] = ms[src])
// duplicates the OOB slice, leaving two slots sharing one buffer; the next
// recvmmsg call then overwrites one slot's control messages with the other's.
// On a dual-stack socket the IPv4 and IPv6 cmsg chains have different lengths,
// so the shared buffer fails to parse and quic-go closes the whole transport,
// silently killing the listener until the process restarts.
func retainMessage(ms []ipv4.Message, dst, src int) {
	if dst == src {
		return
	}
	ms[dst], ms[src] = ms[src], ms[dst]
}

func (c *oobConn) ReadBatch(ms []ipv4.Message, flags int) (int, error) {
	for {
		n, err := c.batch.ReadBatch(ms, flags)
		if err != nil || n == 0 {
			return n, err
		}

		out := 0
		for i := 0; i < n; i++ {
			m := &ms[i]
			if m.N <= 0 || len(m.Buffers) == 0 {
				continue
			}
			data := m.Buffers[0][:m.N]
			src, headerLen, hasHeader, perr := parseV2(data)
			if perr != nil {
				continue
			}

			if !hasHeader {
				transport, ok := m.Addr.(*net.UDPAddr)
				if !ok {
					continue
				}
				if e := c.lookup(transport); e != nil {
					m.Addr = e.logical
				} else {
					c.recordIdentity(transport)
				}
				retainMessage(ms, out, i)
				out++
				continue
			}

			payload := data[headerLen:]
			if len(payload) == 0 {
				continue
			}
			m.Buffers[0] = payload
			m.N = len(payload)
			if logical := c.record(m.Addr, src); logical != nil {
				m.Addr = logical
			}
			retainMessage(ms, out, i)
			out++
		}

		if out > 0 {
			return out, nil
		}
	}
}

func (c *oobConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if transport := c.transportFor(addr); transport != nil {
		return c.UDPConn.WriteTo(p, transport)
	}
	return c.UDPConn.WriteTo(p, addr)
}

func (c *oobConn) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (n, oobn int, err error) {
	if transport := c.transportFor(addr); transport != nil {
		return c.UDPConn.WriteMsgUDP(b, oob, transport)
	}
	return c.UDPConn.WriteMsgUDP(b, oob, addr)
}

type basicConn struct {
	net.PacketConn
	base
}

func newBasicConn(pc net.PacketConn) *basicConn {
	c := &basicConn{PacketConn: pc}
	c.base.init()
	return c
}

func (c *basicConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, transport, err := c.PacketConn.ReadFrom(p)
		if err != nil {
			return 0, nil, err
		}
		src, headerLen, hasHeader, perr := parseV2(p[:n])
		if perr != nil {
			continue
		}
		if !hasHeader {
			if e := c.lookup(transport); e != nil {
				return n, e.logical, nil
			}
			if ua, ok := transport.(*net.UDPAddr); ok {
				c.recordIdentity(ua)
			}
			return n, transport, nil
		}

		copy(p, p[headerLen:n])
		n -= headerLen
		if n == 0 {
			continue
		}
		var logical net.Addr = c.record(transport, src)
		if logical == nil {
			logical = transport
		}
		return n, logical, nil
	}
}

func (c *basicConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if transport := c.transportFor(addr); transport != nil {
		return c.PacketConn.WriteTo(p, transport)
	}
	return c.PacketConn.WriteTo(p, addr)
}

func (c *basicConn) SetReadBuffer(n int) error {
	if s, ok := c.PacketConn.(interface{ SetReadBuffer(int) error }); ok {
		return s.SetReadBuffer(n)
	}
	return errors.New("SetReadBuffer is not supported")
}

func (c *basicConn) SetWriteBuffer(n int) error {
	if s, ok := c.PacketConn.(interface{ SetWriteBuffer(int) error }); ok {
		return s.SetWriteBuffer(n)
	}
	return errors.New("SetWriteBuffer is not supported")
}
