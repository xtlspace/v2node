package limiter

import (
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
	"github.com/wyx2685/v2node/common/rate"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	Nodetype      string      // Node type, e.g. "vless", "shadowsocks"
	SpeedLimit    int         // Node speed limit in Mbps, 0 means unlimited
	UserOnlineIP  *sync.Map   // Key: TagUUID, value: *sync.Map {Key: normalized IP, value: *IPState}
	OldUserOnline *sync.Map   // Key: normalized IP, value: Uid
	UserLimitInfo *sync.Map   // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map   // key: TagUUID, value: *DynamicBucket
	AliveList     map[int]int // Key: Uid, value: alive_ip
	aliveLock     sync.RWMutex
	oldOnlineLock sync.RWMutex
	userLimitLock sync.Mutex
}

type UserLimitInfo struct {
	UID               int
	SpeedLimit        int
	DeviceLimit       int
	DynamicSpeedLimit int
	ExpireTime        int64
	OverLimit         bool
}

// IPState tracks per-device traffic for the current reporting interval.
// IPv6 addresses are aggregated by /64 before being stored here.
type IPState struct {
	UID  int
	Up   atomic.Int64
	Down atomic.Int64
}

func AddLimiter(nodetype string, tag string, speedLimit int, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	l := &Limiter{
		Nodetype:      nodetype,
		SpeedLimit:    speedLimit,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		OldUserOnline: new(sync.Map),
	}
	for i := range users {
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	limitLock.Lock()
	limiter[tag] = l
	limitLock.Unlock()
	return l
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo, modified []panel.UserInfo) {
	for i := range deleted {
		taguuid := format.UserTag(tag, deleted[i].Uuid)
		l.deleteUserLimit(taguuid)
		l.UserOnlineIP.Delete(taguuid)
		l.SpeedLimiter.Delete(taguuid)
		l.DeleteAlive(deleted[i].Id)
	}
	for i := range modified {
		taguuid := format.UserTag(tag, modified[i].Uuid)
		l.updateUserLimit(taguuid, modified[i].SpeedLimit, modified[i].DeviceLimit)
		limit := int64(determineSpeedLimit(l.SpeedLimit, modified[i].SpeedLimit)) * 1000000 / 8
		if limit > 0 {
			if v, ok := l.SpeedLimiter.Load(taguuid); ok {
				d := v.(*rate.DynamicBucket)
				d.Update(limit)
			} else {
				d := rate.NewDynamicBucket(limit)
				l.SpeedLimiter.Store(taguuid, d)
			}
		} else {
			l.SpeedLimiter.Delete(taguuid)
		}
	}
	for i := range added {
		userLimit := &UserLimitInfo{
			UID: added[i].Id,
		}
		if added[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = added[i].SpeedLimit
			userLimit.ExpireTime = 0
		}
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.addUserLimit(format.UserTag(tag, added[i].Uuid), userLimit)
	}
}

func (l *Limiter) SetAliveList(alive map[int]int) {
	l.aliveLock.Lock()
	l.AliveList = alive
	l.aliveLock.Unlock()
}

func (l *Limiter) GetAlive(uid int) int {
	l.aliveLock.RLock()
	defer l.aliveLock.RUnlock()
	return l.AliveList[uid]
}

func (l *Limiter) DeleteAlive(uid int) {
	l.aliveLock.Lock()
	delete(l.AliveList, uid)
	l.aliveLock.Unlock()
}

func (l *Limiter) addUserLimit(taguuid string, info *UserLimitInfo) {
	l.userLimitLock.Lock()
	l.UserLimitInfo.Store(taguuid, info)
	l.userLimitLock.Unlock()
}

func (l *Limiter) updateUserLimit(taguuid string, speedLimit, deviceLimit int) {
	l.userLimitLock.Lock()
	defer l.userLimitLock.Unlock()
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return
	}
	old := v.(*UserLimitInfo)
	next := *old
	next.SpeedLimit = speedLimit
	next.DeviceLimit = deviceLimit
	l.UserLimitInfo.Store(taguuid, &next)
}

func (l *Limiter) deleteUserLimit(taguuid string) {
	l.userLimitLock.Lock()
	l.UserLimitInfo.Delete(taguuid)
	l.userLimitLock.Unlock()
}

// expireUserLimit clears an expired dynamic speed limit and returns the
// effective user speed limit for this connection.
func (l *Limiter) expireUserLimit(taguuid string) int {
	l.userLimitLock.Lock()
	defer l.userLimitLock.Unlock()
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return 0
	}
	u := v.(*UserLimitInfo)
	if u.ExpireTime == 0 || u.ExpireTime >= time.Now().Unix() {
		return determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
	}
	if u.SpeedLimit == 0 {
		l.UserLimitInfo.Delete(taguuid)
		return 0
	}
	next := *u
	next.DynamicSpeedLimit = 0
	next.ExpireTime = 0
	l.UserLimitInfo.Store(taguuid, &next)
	return next.SpeedLimit
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	taguuid := format.UserTag(tag, uuid)
	l.userLimitLock.Lock()
	defer l.userLimitLock.Unlock()
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return errors.New("not found")
	}
	old := v.(*UserLimitInfo)
	next := *old
	next.DynamicSpeedLimit = limit
	next.ExpireTime = expire.Unix()
	l.UserLimitInfo.Store(taguuid, &next)
	return nil
}

func (l *Limiter) CheckLimit(taguuid string, ip netip.Addr, noUDPsource bool) (DynamicBucket *rate.DynamicBucket, ipState *IPState, Reject bool) {
	// Normalize IPv4-mapped IPv6 and aggregate IPv6 by /64.
	ip = normalizeDeviceIP(ip)

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		if u.ExpireTime != 0 && u.ExpireTime < time.Now().Unix() {
			userLimit = l.expireUserLimit(taguuid)
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
		}
	} else {
		return nil, nil, true
	}
	if noUDPsource || l.Nodetype == "hysteria2" || l.Nodetype == "tuic" {
		// Store online user for device limit
		aliveIp := l.GetAlive(uid)
		var reject bool
		ipState, reject = l.trackOnlineIP(taguuid, ip, uid, deviceLimit, aliveIp)
		if reject {
			return nil, nil, true
		}
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	if limit > 0 {
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			return v.(*rate.DynamicBucket), ipState, false
		}
		d := rate.NewDynamicBucket(limit)
		l.SpeedLimiter.Store(taguuid, d)
		return d, ipState, false
	}
	return nil, ipState, false
}

// normalizeDeviceIP maps IPv4-mapped addresses to IPv4 and aggregates IPv6
// addresses by their /64 prefix so that one IPv6 /64 counts as one device.
func normalizeDeviceIP(addr netip.Addr) netip.Addr {
	if !addr.IsValid() {
		return addr
	}
	addr = addr.Unmap()
	if addr.Is4() {
		return addr
	}
	if addr.Zone() != "" {
		addr = addr.WithZone("")
	}
	prefix, err := addr.Prefix(64)
	if err != nil {
		return addr
	}
	return prefix.Addr()
}

// consumeOldOnline reports whether the IP was online in the previous interval.
// If it belongs to the same user, the entry is removed so it only bypasses the
// device limit once.
func (l *Limiter) consumeOldOnline(ip netip.Addr, uid int) bool {
	l.oldOnlineLock.Lock()
	defer l.oldOnlineLock.Unlock()
	if l.OldUserOnline == nil {
		return false
	}
	v, ok := l.OldUserOnline.Load(ip)
	if !ok {
		return false
	}
	if v.(int) == uid {
		l.OldUserOnline.Delete(ip)
	}
	return true
}

func (l *Limiter) setOldOnline(next *sync.Map) {
	l.oldOnlineLock.Lock()
	l.OldUserOnline = next
	l.oldOnlineLock.Unlock()
}

func (l *Limiter) trackOnlineIP(taguuid string, ip netip.Addr, uid, deviceLimit, aliveIp int) (*IPState, bool) {
	if v, loaded := l.UserOnlineIP.Load(taguuid); loaded {
		return l.addOnlineIP(v.(*sync.Map), ip, uid, deviceLimit, aliveIp)
	}
	ipMap := new(sync.Map)
	state := &IPState{UID: uid}
	ipMap.Store(ip, state)
	if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, ipMap); loaded {
		return l.addOnlineIP(v.(*sync.Map), ip, uid, deviceLimit, aliveIp)
	}
	if !l.consumeOldOnline(ip, uid) && deviceLimit > 0 && deviceLimit <= aliveIp {
		l.UserOnlineIP.Delete(taguuid)
		return nil, true
	}
	return state, false
}

// addOnlineIP records a new IP for the user and reports whether the connection
// should be rejected by the device limit.
func (l *Limiter) addOnlineIP(ipMap *sync.Map, ip netip.Addr, uid, deviceLimit, aliveIp int) (*IPState, bool) {
	if v, ok := ipMap.Load(ip); ok {
		return v.(*IPState), false
	}
	state := &IPState{UID: uid}
	if v, loaded := ipMap.LoadOrStore(ip, state); loaded {
		return v.(*IPState), false
	}
	if l.consumeOldOnline(ip, uid) {
		return state, false
	}
	if deviceLimit > 0 && deviceLimit <= aliveIp {
		ipMap.Delete(ip)
		return nil, true
	}
	return state, false
}

func (l *Limiter) GetOnlineDevice(minTrafficKB int) (*[]panel.OnlineUser, error) {
	var onlineUser []panel.OnlineUser
	minBytes := int64(minTrafficKB) * 1000
	next := new(sync.Map)
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			ip := key.(netip.Addr)
			state := value.(*IPState)
			next.Store(ip, state.UID)
			if minTrafficKB > 0 && state.Up.Load()+state.Down.Load() < minBytes {
				return true
			}
			onlineUser = append(onlineUser, panel.OnlineUser{UID: state.UID, IP: ip.String()})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device
		return true
	})
	l.setOldOnline(next)

	return &onlineUser, nil
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}
