package limiter

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
)

func TestAliveListAccess(t *testing.T) {
	l := &Limiter{}
	l.SetAliveList(map[int]int{1: 3})
	if got := l.GetAlive(1); got != 3 {
		t.Fatalf("GetAlive(1) = %d, want 3", got)
	}
	l.DeleteAlive(1)
	if got := l.GetAlive(1); got != 0 {
		t.Fatalf("GetAlive(1) after delete = %d, want 0", got)
	}
}

func TestAliveListConcurrentAccess(t *testing.T) {
	l := &Limiter{AliveList: map[int]int{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				l.SetAliveList(map[int]int{uid: j})
				_ = l.GetAlive(uid)
				l.DeleteAlive(uid)
			}
		}(i)
	}
	wg.Wait()
}

func TestLimiterConcurrentAccess(t *testing.T) {
	Init()
	l := AddLimiter("vless", "node-race", 0, []panel.UserInfo{{
		Id: 1, Uuid: "uuid-race", SpeedLimit: 10, DeviceLimit: 5,
	}}, map[int]int{1: 0})
	defer DeleteLimiter("node-race")

	tag := format.UserTag("node-race", "uuid-race")
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				fn()
			}
		}()
	}

	run(func() { _, _, _ = l.CheckLimit(tag, netip.MustParseAddr("1.2.3.4"), true) })
	run(func() { _, _ = l.GetOnlineDevice(0) })
	run(func() {
		l.UpdateUser("node-race", nil, nil, []panel.UserInfo{{
			Id: 1, Uuid: "uuid-race", SpeedLimit: 20, DeviceLimit: 6,
		}})
	})
	run(func() {
		_ = l.UpdateDynamicSpeedLimit("node-race", "uuid-race", 30, time.Now().Add(time.Hour))
	})

	wg.Wait()
}

func TestNormalizeDeviceIP(t *testing.T) {
	tests := []struct {
		name string
		ip   netip.Addr
		want netip.Addr
	}{
		{"ipv4", netip.MustParseAddr("1.2.3.4"), netip.MustParseAddr("1.2.3.4")},
		{"ipv4 mapped", netip.MustParseAddr("::ffff:1.2.3.4"), netip.MustParseAddr("1.2.3.4")},
		{"ipv6 /64", netip.MustParseAddr("2001:db8:1:2:3:4:5:6"), netip.MustParseAddr("2001:db8:1:2::")},
		{"ipv6 /64 second host", netip.MustParseAddr("2001:db8:1:2:ffff::1"), netip.MustParseAddr("2001:db8:1:2::")},
		{"ipv6 loopback", netip.MustParseAddr("::1"), netip.MustParseAddr("::")},
		{"invalid", netip.Addr{}, netip.Addr{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeDeviceIP(tt.ip); got != tt.want {
				t.Fatalf("normalizeDeviceIP(%v) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestCheckLimitAggregatesIPv6By64(t *testing.T) {
	Init()
	l := AddLimiter("vless", "node-v6", 0, []panel.UserInfo{{
		Id: 1, Uuid: "uuid-v6", DeviceLimit: 10,
	}}, map[int]int{1: 0})
	defer DeleteLimiter("node-v6")

	tag := format.UserTag("node-v6", "uuid-v6")
	_, state1, reject := l.CheckLimit(tag, netip.MustParseAddr("2001:db8:1:2::1"), true)
	if reject || state1 == nil {
		t.Fatalf("first connect: reject=%v state=%v", reject, state1)
	}
	_, state2, reject := l.CheckLimit(tag, netip.MustParseAddr("2001:db8:1:2::ffff"), true)
	if reject || state2 == nil {
		t.Fatalf("second connect: reject=%v state=%v", reject, state2)
	}
	if state1 != state2 {
		t.Fatal("addresses in the same /64 must share one IPState")
	}

	state1.Up.Add(600_000)
	online, _ := l.GetOnlineDevice(500)
	if len(*online) != 1 {
		t.Fatalf("online devices = %d, want 1", len(*online))
	}
	if (*online)[0].IP != "2001:db8:1:2::" {
		t.Fatalf("reported IP = %q, want masked /64", (*online)[0].IP)
	}

	// The per-IP counters are reset when the interval is reported.
	online, _ = l.GetOnlineDevice(500)
	if len(*online) != 0 {
		t.Fatalf("online devices after reset = %d, want 0", len(*online))
	}
}

func TestGetOnlineDeviceFiltersByPerIPTraffic(t *testing.T) {
	Init()
	l := AddLimiter("vless", "node-perip", 0, []panel.UserInfo{{
		Id: 1, Uuid: "uuid-perip", DeviceLimit: 10,
	}}, map[int]int{1: 0})
	defer DeleteLimiter("node-perip")

	tag := format.UserTag("node-perip", "uuid-perip")
	_, high, _ := l.CheckLimit(tag, netip.MustParseAddr("1.2.3.4"), true)
	_, low, _ := l.CheckLimit(tag, netip.MustParseAddr("5.6.7.8"), true)
	if high == nil || low == nil {
		t.Fatal("expected IPState for both connections")
	}
	high.Up.Add(600_000)
	low.Up.Add(50_000)

	online, _ := l.GetOnlineDevice(500)
	if len(*online) != 1 {
		t.Fatalf("online devices = %d, want 1", len(*online))
	}
	if (*online)[0].IP != "1.2.3.4" {
		t.Fatalf("reported IP = %q, want 1.2.3.4", (*online)[0].IP)
	}
}

func TestGetOnlineDeviceWithoutFilter(t *testing.T) {
	Init()
	l := AddLimiter("vless", "node-nofilter", 0, []panel.UserInfo{{
		Id: 1, Uuid: "uuid-nofilter", DeviceLimit: 10,
	}}, map[int]int{1: 0})
	defer DeleteLimiter("node-nofilter")

	tag := format.UserTag("node-nofilter", "uuid-nofilter")
	if _, _, reject := l.CheckLimit(tag, netip.MustParseAddr("1.2.3.4"), true); reject {
		t.Fatal("user rejected unexpectedly")
	}
	if _, _, reject := l.CheckLimit(tag, netip.MustParseAddr("5.6.7.8"), true); reject {
		t.Fatal("user rejected unexpectedly")
	}

	online, _ := l.GetOnlineDevice(0)
	if len(*online) != 2 {
		t.Fatalf("online devices = %d, want 2", len(*online))
	}
}
