package proxyprotocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"
)

func buildV2(t *testing.T, src, dst netip.AddrPort) []byte {
	t.Helper()
	var b []byte
	b = append(b, v2Signature...)
	b = append(b, 0x21)

	if src.Addr().Is4() {
		b = append(b, 0x12, 0, 12)
		b = append(b, src.Addr().AsSlice()...)
		b = append(b, dst.Addr().AsSlice()...)
		b = binary.BigEndian.AppendUint16(b, src.Port())
		b = binary.BigEndian.AppendUint16(b, dst.Port())
		return b
	}

	b = append(b, 0x22, 0, 36)
	b = append(b, src.Addr().AsSlice()...)
	b = append(b, dst.Addr().AsSlice()...)
	b = binary.BigEndian.AppendUint16(b, src.Port())
	b = binary.BigEndian.AppendUint16(b, dst.Port())
	return b
}

func TestParseV2IPv4(t *testing.T) {
	src := netip.MustParseAddrPort("1.2.3.4:1234")
	dst := netip.MustParseAddrPort("5.6.7.8:443")
	header := buildV2(t, src, dst)
	datagram := append(header, []byte("hello")...)

	got, n, hasHeader, err := parseV2(datagram)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHeader {
		t.Fatal("expected header")
	}
	if n != len(header) {
		t.Fatalf("header length = %d, want %d", n, len(header))
	}
	if got != src {
		t.Fatalf("source = %v, want %v", got, src)
	}
}

func TestParseV2IPv6(t *testing.T) {
	src := netip.MustParseAddrPort("[2001:db8::1]:1234")
	dst := netip.MustParseAddrPort("[2001:db8::2]:443")
	header := buildV2(t, src, dst)
	datagram := append(header, []byte("hello")...)

	got, n, hasHeader, err := parseV2(datagram)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHeader {
		t.Fatal("expected header")
	}
	if n != len(header) {
		t.Fatalf("header length = %d, want %d", n, len(header))
	}
	if got != src {
		t.Fatalf("source = %v, want %v", got, src)
	}
}

func TestParseV2NoHeader(t *testing.T) {
	_, _, hasHeader, err := parseV2([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if hasHeader {
		t.Fatal("did not expect a header")
	}
}

func TestParseV21Rejected(t *testing.T) {
	_, _, _, err := parseV2([]byte("PROXY TCP4 1.2.3.4 5.6.7.8 1234 443\r\n"))
	if !errorsIs(err, errV1Header) {
		t.Fatalf("err = %v, want v1 error", err)
	}
}

func TestParseV2Malformed(t *testing.T) {
	datagram := append([]byte{}, v2Signature...)
	datagram = append(datagram, 0x21, 0x12, 0, 12)
	datagram = append(datagram, 1, 2, 3) // truncated address block

	if _, _, _, err := parseV2(datagram); err == nil {
		t.Fatal("expected error")
	}
}

func errorsIs(err, target error) bool {
	return err == target
}

type fakeConn struct {
	reads  [][]byte
	addrs  []net.Addr
	writes [][]byte
	waddrs []net.Addr
}

func (f *fakeConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if len(f.reads) == 0 {
		return 0, nil, io.EOF
	}
	n := copy(p, f.reads[0])
	f.reads = f.reads[1:]
	addr := f.addrs[0]
	f.addrs = f.addrs[1:]
	return n, addr, nil
}

func (f *fakeConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	f.writes = append(f.writes, append([]byte(nil), p...))
	f.waddrs = append(f.waddrs, addr)
	return len(p), nil
}

func (f *fakeConn) Close() error                     { return nil }
func (f *fakeConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func TestOptionalMapping(t *testing.T) {
	transport := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 40000}
	logical := &net.UDPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 1234}
	header := buildV2(t, netip.MustParseAddrPort("1.2.3.4:1234"), netip.MustParseAddrPort("5.6.7.8:443"))

	fake := &fakeConn{
		reads: [][]byte{append(header, []byte("hello")...)},
		addrs: []net.Addr{transport},
	}
	conn := Wrap(fake)

	buf := make([]byte, 64)
	n, addr, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("payload = %q, want hello", buf[:n])
	}
	ua, ok := addr.(*net.UDPAddr)
	if !ok || !ua.IP.Equal(logical.IP) || ua.Port != logical.Port {
		t.Fatalf("logical addr = %v, want %v", addr, logical)
	}

	if _, err := conn.WriteTo([]byte("pong"), addr); err != nil {
		t.Fatal(err)
	}
	if len(fake.waddrs) != 1 || fake.waddrs[0].String() != transport.String() {
		t.Fatalf("write addr = %v, want %v", fake.waddrs, transport)
	}
}

func TestOptionalNoHeader(t *testing.T) {
	transport := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 40000}
	fake := &fakeConn{
		reads: [][]byte{[]byte("raw-udp")},
		addrs: []net.Addr{transport},
	}
	conn := Wrap(fake)

	buf := make([]byte, 64)
	n, addr, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "raw-udp" {
		t.Fatalf("payload = %q, want raw-udp", buf[:n])
	}
	if addr.String() != transport.String() {
		t.Fatalf("addr = %v, want %v", addr, transport)
	}
}

func TestV1Dropped(t *testing.T) {
	transport := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 40000}
	fake := &fakeConn{
		reads: [][]byte{
			[]byte("PROXY TCP4 1.2.3.4 5.6.7.8 1234 443\r\n"),
			[]byte("raw-udp"),
		},
		addrs: []net.Addr{transport, transport},
	}
	conn := Wrap(fake)

	buf := make([]byte, 64)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], []byte("raw-udp")) {
		t.Fatalf("payload = %q, want raw-udp", buf[:n])
	}
}
