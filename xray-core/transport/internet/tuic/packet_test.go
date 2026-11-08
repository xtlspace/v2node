package tuic

import (
	"bytes"
	"testing"

	xnet "github.com/xtls/xray-core/common/net"
)

func mustDestination(t *testing.T, network xnet.Network, address string, port uint16) xnet.Destination {
	t.Helper()
	dest := xnet.Destination{
		Network: network,
		Address: xnet.ParseAddress(address),
		Port:    xnet.Port(port),
	}
	if !dest.IsValid() {
		t.Fatalf("invalid test destination %q:%d", address, port)
	}
	return dest
}

func TestUDPMessagePackGolden(t *testing.T) {
	tests := []struct {
		name string
		msg  udpMessage
		want []byte
	}{
		{
			name: "ipv4",
			msg: udpMessage{
				sessionID:     0x1234,
				packetID:      0x5678,
				fragmentTotal: 1,
				destination:   mustDestination(t, xnet.Network_UDP, "1.2.3.4", 80),
				data:          []byte("abc"),
			},
			want: []byte{
				0x05, 0x02, // version, command
				0x12, 0x34, // session id
				0x56, 0x78, // packet id
				0x01,       // fragment total
				0x00,       // fragment id
				0x00, 0x03, // payload length
				0x01, 1, 2, 3, 4, // IPv4
				0x00, 0x50, // port 80
				'a', 'b', 'c',
			},
		},
		{
			name: "domain",
			msg: udpMessage{
				sessionID:     9,
				packetID:      10,
				fragmentTotal: 1,
				destination:   mustDestination(t, xnet.Network_UDP, "example.com", 53),
				data:          []byte("hi"),
			},
			want: []byte{
				0x05, 0x02,
				0x00, 0x09,
				0x00, 0x0a,
				0x01,
				0x00,
				0x00, 0x02,
				0x00, 0x0b,
				'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
				0x00, 0x35,
				'h', 'i',
			},
		},
		{
			name: "ipv6",
			msg: udpMessage{
				sessionID:     1,
				packetID:      2,
				fragmentTotal: 7,
				fragmentID:    3,
				destination:   mustDestination(t, xnet.Network_UDP, "2001:db8::1", 443),
				data:          []byte("x"),
			},
			want: []byte{
				0x05, 0x02,
				0x00, 0x01,
				0x00, 0x02,
				0x07,
				0x03,
				0x00, 0x01,
				0x02,
				0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x01, 0xbb,
				'x',
			},
		},
		{
			name: "no destination",
			msg: udpMessage{
				sessionID:     1,
				packetID:      2,
				fragmentTotal: 1,
				data:          []byte("abc"),
			},
			want: []byte{
				0x05, 0x02,
				0x00, 0x01,
				0x00, 0x02,
				0x01,
				0x00,
				0x00, 0x03,
				0xff,
				'a', 'b', 'c',
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.msg.pack()
			if err != nil {
				t.Fatalf("pack() error: %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("pack() mismatch\n got: %x\nwant: %x", got, test.want)
			}
			if len(got) != test.msg.headerSize()+len(test.msg.data) {
				t.Fatalf("headerSize() = %d, packed length = %d", test.msg.headerSize(), len(got))
			}
		})
	}
}

func TestDecodeUDPMessageRoundTrip(t *testing.T) {
	original := &udpMessage{
		sessionID:     0xbeef,
		packetID:      0x0042,
		fragmentTotal: 1,
		destination:   mustDestination(t, xnet.Network_UDP, "example.org", 443),
		data:          bytes.Repeat([]byte("0123456789"), 32),
	}
	packed, err := original.pack()
	if err != nil {
		t.Fatalf("pack() error: %v", err)
	}

	// handleMessage passes the datagram after the version and command bytes.
	var decoded udpMessage
	if err := decodeUDPMessage(&decoded, packed[2:]); err != nil {
		t.Fatalf("decodeUDPMessage() error: %v", err)
	}
	assertUDPMessageEqual(t, original, &decoded)

	// The payload is retained as a sub-slice of the datagram (no extra copy).
	if len(decoded.data) == 0 || &decoded.data[0] != &packed[original.headerSize()] {
		t.Fatalf("decoded payload is not a sub-slice of the datagram (len=%d)", len(decoded.data))
	}
}

func TestReadUDPMessageRoundTrip(t *testing.T) {
	original := &udpMessage{
		sessionID:     7,
		packetID:      8,
		fragmentTotal: 2,
		fragmentID:    1,
		destination:   mustDestination(t, xnet.Network_UDP, "2001:db8::2", 53),
		data:          []byte("payload"),
	}
	packed, err := original.pack()
	if err != nil {
		t.Fatalf("pack() error: %v", err)
	}

	var decoded udpMessage
	if err := readUDPMessage(&decoded, bytes.NewReader(packed[2:])); err != nil {
		t.Fatalf("readUDPMessage() error: %v", err)
	}
	assertUDPMessageEqual(t, original, &decoded)
}

func TestDecodeUDPMessageRejectsTruncatedPayload(t *testing.T) {
	original := &udpMessage{
		sessionID:     1,
		packetID:      2,
		fragmentTotal: 1,
		destination:   mustDestination(t, xnet.Network_UDP, "1.2.3.4", 80),
		data:          []byte("abc"),
	}
	packed, err := original.pack()
	if err != nil {
		t.Fatalf("pack() error: %v", err)
	}
	var decoded udpMessage
	if err := decodeUDPMessage(&decoded, packed[2:len(packed)-1]); err == nil {
		t.Fatal("decodeUDPMessage() accepted a truncated payload")
	}
}

func assertUDPMessageEqual(t *testing.T, want, got *udpMessage) {
	t.Helper()
	if got.sessionID != want.sessionID {
		t.Fatalf("sessionID = %d, want %d", got.sessionID, want.sessionID)
	}
	if got.packetID != want.packetID {
		t.Fatalf("packetID = %d, want %d", got.packetID, want.packetID)
	}
	if got.fragmentTotal != want.fragmentTotal {
		t.Fatalf("fragmentTotal = %d, want %d", got.fragmentTotal, want.fragmentTotal)
	}
	if got.fragmentID != want.fragmentID {
		t.Fatalf("fragmentID = %d, want %d", got.fragmentID, want.fragmentID)
	}
	if got.destination.Network != want.destination.Network ||
		got.destination.Address.String() != want.destination.Address.String() ||
		got.destination.Port != want.destination.Port {
		t.Fatalf("destination = %v, want %v", got.destination, want.destination)
	}
	if !bytes.Equal(got.data, want.data) {
		t.Fatalf("data = %q, want %q", got.data, want.data)
	}
}

func TestUDPDefraggerReassembly(t *testing.T) {
	dest := mustDestination(t, xnet.Network_UDP, "1.1.1.1", 53)
	first := &udpMessage{sessionID: 1, packetID: 9, fragmentTotal: 3, fragmentID: 0, destination: dest, data: []byte("aaa")}
	second := &udpMessage{sessionID: 1, packetID: 9, fragmentTotal: 3, fragmentID: 1, data: []byte("bbb")}
	third := &udpMessage{sessionID: 1, packetID: 9, fragmentTotal: 3, fragmentID: 2, data: []byte("ccc")}

	d := newUDPDefragger()
	if got := d.feed(first); got != nil {
		t.Fatalf("first fragment reassembled early: %v", got)
	}
	if got := d.feed(first); got != nil {
		t.Fatalf("duplicate fragment reassembled early: %v", got)
	}
	if got := d.feed(third); got != nil {
		t.Fatalf("out-of-order fragment reassembled early: %v", got)
	}
	got := d.feed(second)
	if got == nil {
		t.Fatal("fragment set was not reassembled")
	}
	if string(got.data) != "aaabbbccc" {
		t.Fatalf("reassembled data = %q", got.data)
	}
	if got.destination.Address.String() != dest.Address.String() || got.destination.Port != dest.Port {
		t.Fatalf("reassembled destination = %v, want %v", got.destination, dest)
	}
	if len(d.packetMap) != 0 || d.lru.Len() != 0 {
		t.Fatalf("defragger retained %d entries / %d lru elements", len(d.packetMap), d.lru.Len())
	}
}

func TestUDPDefraggerEviction(t *testing.T) {
	d := newUDPDefragger()
	for i := 0; i < udpFragmentMaxEntries; i++ {
		if got := d.feed(&udpMessage{packetID: uint16(i), fragmentTotal: 2, fragmentID: 0, data: []byte{byte(i)}}); got != nil {
			t.Fatalf("unexpected reassembly for packet %d", i)
		}
	}
	if len(d.packetMap) != udpFragmentMaxEntries {
		t.Fatalf("packetMap length = %d, want %d", len(d.packetMap), udpFragmentMaxEntries)
	}
	// One more incomplete set evicts the least recently seen entry (packet 0).
	if got := d.feed(&udpMessage{packetID: uint16(udpFragmentMaxEntries), fragmentTotal: 2, fragmentID: 0, data: []byte("new")}); got != nil {
		t.Fatal("unexpected reassembly for the new packet")
	}
	if _, ok := d.packetMap[0]; ok {
		t.Fatal("oldest fragment set was not evicted")
	}
	if len(d.packetMap) != udpFragmentMaxEntries || d.lru.Len() != udpFragmentMaxEntries {
		t.Fatalf("map/lru size = %d/%d, want %d/%d", len(d.packetMap), d.lru.Len(), udpFragmentMaxEntries, udpFragmentMaxEntries)
	}

	// The retained sets can still be completed.
	got := d.feed(&udpMessage{packetID: uint16(udpFragmentMaxEntries), fragmentTotal: 2, fragmentID: 1, data: []byte("done")})
	if got == nil || string(got.data) != "newdone" {
		t.Fatalf("reassembly after eviction = %v", got)
	}
}

func TestUDPDefraggerRejectsInvalidFragment(t *testing.T) {
	d := newUDPDefragger()
	if got := d.feed(&udpMessage{packetID: 1, fragmentTotal: 2, fragmentID: 2}); got != nil {
		t.Fatal("invalid fragment id was accepted")
	}
	if len(d.packetMap) != 0 {
		t.Fatal("invalid fragment created state")
	}
}

func BenchmarkUDPMessagePack(b *testing.B) {
	msg := &udpMessage{
		sessionID:     1,
		packetID:      2,
		fragmentTotal: 1,
		destination:   xnet.UDPDestination(xnet.ParseAddress("example.com"), 443),
		data:          bytes.Repeat([]byte("x"), 1200),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.pack(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUDPMessage(b *testing.B) {
	msg := &udpMessage{
		sessionID:     1,
		packetID:      2,
		fragmentTotal: 1,
		destination:   xnet.UDPDestination(xnet.ParseAddress("example.com"), 443),
		data:          bytes.Repeat([]byte("x"), 1200),
	}
	packed, err := msg.pack()
	if err != nil {
		b.Fatal(err)
	}
	body := packed[2:]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded udpMessage
		if err := decodeUDPMessage(&decoded, body); err != nil {
			b.Fatal(err)
		}
	}
}
