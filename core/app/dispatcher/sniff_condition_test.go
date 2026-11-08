package dispatcher

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

func TestShouldSniff(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		dest     net.Destination
		expected bool
	}{
		{
			name:     "disabled",
			enabled:  false,
			dest:     net.TCPDestination(net.ParseAddress("1.2.3.4"), 443),
			expected: false,
		},
		{
			name:     "ipv4 destination",
			enabled:  true,
			dest:     net.TCPDestination(net.ParseAddress("1.2.3.4"), 443),
			expected: true,
		},
		{
			name:     "ipv6 destination",
			enabled:  true,
			dest:     net.TCPDestination(net.ParseAddress("2001:db8::1"), 443),
			expected: true,
		},
		{
			name:     "domain destination",
			enabled:  true,
			dest:     net.TCPDestination(net.ParseAddress("example.com"), 443),
			expected: false,
		},
		{
			name:     "domain destination over udp",
			enabled:  true,
			dest:     net.UDPDestination(net.ParseAddress("example.com"), 443),
			expected: false,
		},
		{
			name:     "nil address",
			enabled:  true,
			dest:     net.Destination{Network: net.Network_TCP},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldSniff(session.SniffingRequest{Enabled: tt.enabled}, tt.dest)
			if got != tt.expected {
				t.Fatalf("shouldSniff() = %v, want %v", got, tt.expected)
			}
		})
	}
}
