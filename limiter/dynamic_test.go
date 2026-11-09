package limiter

import (
	"net/netip"
	"testing"

	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
)

func TestDetermineSpeedLimit(t *testing.T) {
	tests := []struct {
		name     string
		node     int
		user     int
		expected int
	}{
		{"node only", 100, 0, 100},
		{"user only", 0, 50, 50},
		{"both unlimited", 0, 0, 0},
		{"node stricter", 100, 50, 50},
		{"user stricter", 30, 80, 30},
		{"equal", 60, 60, 60},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := determineSpeedLimit(tt.node, tt.user); got != tt.expected {
				t.Fatalf("determineSpeedLimit(%d, %d) = %d, expected %d", tt.node, tt.user, got, tt.expected)
			}
		})
	}
}

func TestAddLimiterNodeSpeedLimit(t *testing.T) {
	Init()

	users := []panel.UserInfo{{Id: 1, Uuid: "uuid-1"}}
	l := AddLimiter("vless", "node-a", 100, users, map[int]int{})
	if l.SpeedLimit != 100 {
		t.Fatalf("node speed limit = %d, expected 100", l.SpeedLimit)
	}

	// A user without its own speed limit should still get a bucket from the node limit.
	bucket, _, reject := l.CheckLimit(format.UserTag("node-a", "uuid-1"), netip.MustParseAddr("1.2.3.4"), true)
	if reject {
		t.Fatal("user rejected unexpectedly")
	}
	if bucket == nil {
		t.Fatal("expected a rate limit bucket from node speed limit")
	}
}

func TestAddLimiterWithoutSpeedLimit(t *testing.T) {
	Init()

	users := []panel.UserInfo{{Id: 2, Uuid: "uuid-2"}}
	l := AddLimiter("vless", "node-b", 0, users, map[int]int{})
	bucket, _, reject := l.CheckLimit(format.UserTag("node-b", "uuid-2"), netip.MustParseAddr("1.2.3.4"), true)
	if reject {
		t.Fatal("user rejected unexpectedly")
	}
	if bucket != nil {
		t.Fatal("expected no rate limit bucket when both node and user limits are 0")
	}
}
