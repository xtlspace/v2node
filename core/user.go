package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/counter"
	"github.com/wyx2685/v2node/common/format"
	"github.com/wyx2685/v2node/core/app/dispatcher"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/anytls"
	hyaccount "github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	"github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"github.com/xtls/xray-core/proxy/tuic"
	"github.com/xtls/xray-core/proxy/vless"
)

func (v *V2Core) GetUserManager(tag string) (proxy.UserManager, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	handler, err := v.ihm.GetHandler(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("no such inbound tag: %s", err)
	}
	inboundInstance, ok := handler.(proxy.GetInbound)
	if !ok {
		return nil, fmt.Errorf("handler %s is not implement proxy.GetInbound", tag)
	}
	userManager, ok := inboundInstance.GetInbound().(proxy.UserManager)
	if !ok {
		return nil, fmt.Errorf("handler %s is not implement proxy.UserManager", tag)
	}
	return userManager, nil
}

func (vc *V2Core) DelUsers(users []panel.UserInfo, tag string, _ *panel.NodeInfo) (deleted []panel.UserInfo, err error) {
	userManager, err := vc.GetUserManager(tag)
	if err != nil {
		return nil, fmt.Errorf("get user manager error: %s", err)
	}
	for i := range users {
		user := format.UserTag(tag, users[i].Uuid)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var removeErr error
		if userManager.GetUser(ctx, user) != nil {
			removeErr = userManager.RemoveUser(ctx, user)
		}
		cancel()
		if removeErr != nil {
			// A concurrent removal, or a protocol that keeps reporting the
			// user after removal, must not block the rest of the batch.
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			stillExists := userManager.GetUser(checkCtx, user) != nil
			checkCancel()
			if stillExists {
				log.WithFields(log.Fields{
					"tag":   tag,
					"email": user,
					"err":   removeErr,
				}).Error("Remove user failed")
				continue
			}
		}
		vc.users.mapLock.Lock()
		delete(vc.users.uidMap, user)
		vc.users.mapLock.Unlock()
		if v, ok := vc.dispatcher.Counter.Load(tag); ok {
			tc := v.(*counter.TrafficCounter)
			tc.Delete(user)
		}
		if v, ok := vc.dispatcher.LinkManagers.Load(user); ok {
			lm := v.(*dispatcher.LinkManager)
			lm.CloseAll()
			vc.dispatcher.LinkManagers.Delete(user)
		}
		deleted = append(deleted, users[i])
	}
	return deleted, nil
}

// UserTrafficSnapshot is a point-in-time view of a user's accumulated traffic.
// It is intentionally not filtered or reset so that callers can use the same
// snapshot for both traffic reporting and online-device filtering.
type UserTrafficSnapshot struct {
	Email    string
	UID      int
	Upload   int64
	Download int64
}

func (vc *V2Core) GetUserTrafficSnapshot(tag string) []UserTrafficSnapshot {
	if vc.dispatcher == nil {
		return nil
	}
	v, ok := vc.dispatcher.Counter.Load(tag)
	if !ok {
		return nil
	}
	c := v.(*counter.TrafficCounter)

	vc.users.mapLock.RLock()
	uidMap := make(map[string]int, len(vc.users.uidMap))
	for email, uid := range vc.users.uidMap {
		uidMap[email] = uid
	}
	vc.users.mapLock.RUnlock()

	snapshots := make([]UserTrafficSnapshot, 0)
	c.Counters.Range(func(key, value interface{}) bool {
		email := key.(string)
		storage := value.(*counter.TrafficStorage)
		up := storage.UpCounter.Load()
		down := storage.DownCounter.Load()
		if up+down <= 0 {
			return true
		}
		uid, ok := uidMap[email]
		if !ok || uid == 0 {
			// The user was deleted; drop the stale counter.
			c.Delete(email)
			return true
		}
		snapshots = append(snapshots, UserTrafficSnapshot{
			Email:    email,
			UID:      uid,
			Upload:   up,
			Download: down,
		})
		return true
	})
	return snapshots
}

// CommitUserTraffic subtracts traffic that was successfully reported. Traffic
// that arrived after the snapshot is preserved.
func (vc *V2Core) CommitUserTraffic(tag string, reported []UserTrafficSnapshot) {
	if vc.dispatcher == nil || len(reported) == 0 {
		return
	}
	v, ok := vc.dispatcher.Counter.Load(tag)
	if !ok {
		return
	}
	c := v.(*counter.TrafficCounter)
	for _, s := range reported {
		value, ok := c.Counters.Load(s.Email)
		if !ok {
			continue
		}
		storage := value.(*counter.TrafficStorage)
		subtractClamped(&storage.UpCounter, s.Upload)
		subtractClamped(&storage.DownCounter, s.Download)
	}
}

func subtractClamped(counter *atomic.Int64, n int64) {
	if n <= 0 {
		return
	}
	for {
		current := counter.Load()
		next := current - n
		if next < 0 {
			next = 0
		}
		if counter.CompareAndSwap(current, next) {
			return
		}
	}
}

func (v *V2Core) AddUsers(p *AddUsersParams) (added []panel.UserInfo, err error) {
	var users []*protocol.User
	switch p.NodeInfo.Type {
	case "vless":
		users = buildVlessUsers(p.Tag, p.Users, p.Common.Flow)
	case "shadowsocks":
		users = buildSSUsers(p.Tag,
			p.Users,
			p.Common.Cipher,
			p.Common.ServerKey)
	case "hysteria2":
		users = buildHysteria2Users(p.Tag, p.Users)
	case "tuic":
		users = buildTuicUsers(p.Tag, p.Users)
	case "anytls":
		users = buildAnyTLSUsers(p.Tag, p.Users)
	default:
		return nil, fmt.Errorf("unsupported node type: %s", p.NodeInfo.Type)
	}
	man, err := v.GetUserManager(p.Tag)
	if err != nil {
		return nil, fmt.Errorf("get user manager error: %s", err)
	}
	for i, u := range users {
		userInfo := p.Users[i]
		user := format.UserTag(p.Tag, userInfo.Uuid)
		mUser, buildErr := u.ToMemoryUser()
		if buildErr != nil {
			log.WithFields(log.Fields{
				"tag":   p.Tag,
				"email": user,
				"err":   buildErr,
			}).Error("Build user failed")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var addErr error
		if man.GetUser(ctx, user) == nil {
			addErr = man.AddUser(ctx, mUser)
		}
		cancel()
		if addErr != nil {
			// Re-check to treat a concurrent add (or "already exists") as success.
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			exists := man.GetUser(checkCtx, user) != nil
			checkCancel()
			if !exists {
				log.WithFields(log.Fields{
					"tag":   p.Tag,
					"email": user,
					"err":   addErr,
				}).Error("Add user failed")
				continue
			}
		}
		v.users.mapLock.Lock()
		v.users.uidMap[user] = userInfo.Id
		v.users.mapLock.Unlock()
		added = append(added, userInfo)
	}
	return added, nil
}

func buildVlessUsers(tag string, userInfo []panel.UserInfo, flow string) (users []*protocol.User) {
	users = make([]*protocol.User, len(userInfo))
	for i := range userInfo {
		users[i] = buildVlessUser(tag, &(userInfo)[i], flow)
	}
	return users
}

func buildVlessUser(tag string, userInfo *panel.UserInfo, flow string) (user *protocol.User) {
	vlessAccount := &vless.Account{
		Id: userInfo.Uuid,
	}
	vlessAccount.Flow = flow
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(vlessAccount),
	}
}

func buildSSUsers(tag string, userInfo []panel.UserInfo, cypher string, serverKey string) (users []*protocol.User) {
	users = make([]*protocol.User, len(userInfo))
	for i := range userInfo {
		users[i] = buildSSUser(tag, &userInfo[i], cypher, serverKey)
	}
	return users
}

func buildSSUser(tag string, userInfo *panel.UserInfo, cypher string, serverKey string) (user *protocol.User) {
	if serverKey == "" {
		ssAccount := &shadowsocks.Account{
			Password:   userInfo.Uuid,
			CipherType: getCipherFromString(cypher),
		}
		return &protocol.User{
			Level:   0,
			Email:   format.UserTag(tag, userInfo.Uuid),
			Account: serial.ToTypedMessage(ssAccount),
		}
	} else {
		var keyLength int
		switch cypher {
		case "2022-blake3-aes-128-gcm":
			keyLength = 16
		case "2022-blake3-aes-256-gcm":
			keyLength = 32
		case "2022-blake3-chacha20-poly1305":
			keyLength = 32
		}
		ssAccount := &shadowsocks_2022.Account{
			Key: base64.StdEncoding.EncodeToString([]byte(userInfo.Uuid[:keyLength])),
		}
		return &protocol.User{
			Level:   0,
			Email:   format.UserTag(tag, userInfo.Uuid),
			Account: serial.ToTypedMessage(ssAccount),
		}
	}
}

func getCipherFromString(c string) shadowsocks.CipherType {
	switch strings.ToLower(c) {
	case "aes-128-gcm", "aead_aes_128_gcm":
		return shadowsocks.CipherType_AES_128_GCM
	case "aes-256-gcm", "aead_aes_256_gcm":
		return shadowsocks.CipherType_AES_256_GCM
	case "chacha20-poly1305", "aead_chacha20_poly1305", "chacha20-ietf-poly1305":
		return shadowsocks.CipherType_CHACHA20_POLY1305
	default:
		return shadowsocks.CipherType_UNKNOWN
	}
}

func buildHysteria2Users(tag string, userInfo []panel.UserInfo) (users []*protocol.User) {
	users = make([]*protocol.User, len(userInfo))
	for i := range userInfo {
		users[i] = buildHysteria2User(tag, &userInfo[i])
	}
	return users
}

func buildHysteria2User(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	hysteria2Account := &hyaccount.Account{
		Auth: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(hysteria2Account),
	}
}

func buildTuicUsers(tag string, userInfo []panel.UserInfo) (users []*protocol.User) {
	users = make([]*protocol.User, len(userInfo))
	for i := range userInfo {
		users[i] = buildTuicUser(tag, &userInfo[i])
	}
	return users
}

func buildTuicUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	tuicAccount := &tuic.Account{
		Uuid:     userInfo.Uuid,
		Password: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(tuicAccount),
	}
}

func buildAnyTLSUsers(tag string, userInfo []panel.UserInfo) (users []*protocol.User) {
	users = make([]*protocol.User, len(userInfo))
	for i := range userInfo {
		users[i] = buildAnyTLSUser(tag, &userInfo[i])
	}
	return users
}

func buildAnyTLSUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	anyTLSAccount := &anytls.Account{
		Password: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(anyTLSAccount),
	}
}
