package node

import (
	"context"
	"errors"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	vCore "github.com/wyx2685/v2node/core"
)

func (c *Controller) reportUserTrafficTask(ctx context.Context) (err error) {
	var reportmin = 0
	var devicemin = 0
	if c.info.Common.BaseConfig != nil {
		reportmin = c.info.Common.BaseConfig.NodeReportMinTraffic
		devicemin = c.info.Common.BaseConfig.DeviceOnlineMinTraffic
	}

	snapshot := c.server.GetUserTrafficSnapshot(c.tag)
	reportTraffic := filterReportTraffic(snapshot, reportmin)
	if len(reportTraffic) > 0 {
		err = c.apiClient.ReportUserTraffic(ctx, toPanelTraffic(reportTraffic))

		// Consume the snapshot regardless of the result. This is intentionally
		// at-most-once: a failed report must not be retried, otherwise a lost
		// response after the panel processed the request would double-bill.
		c.server.CommitUserTraffic(c.tag, reportTraffic)

		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Info("Report user traffic failed, traffic already consumed")
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		} else {
			log.WithField("tag", c.tag).Infof("Report %d users traffic", len(reportTraffic))
		}
	}

	if onlineDevice, err := c.limiter.GetOnlineDevice(devicemin); err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Info("Get online device failed")
	} else if len(*onlineDevice) > 0 {
		data := make(map[int][]string)
		for _, onlineuser := range *onlineDevice {
			// json structure: { UID1:["ip1","ip2"],UID2:["ip3","ip4"] }
			data[onlineuser.UID] = append(data[onlineuser.UID], onlineuser.IP)
		}
		if len(data) != 0 {
			err := c.apiClient.ReportNodeOnlineUsers(ctx, &data)
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Info("Report online users failed")
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
			}
		}
		log.WithField("tag", c.tag).Infof("Total %d online users", len(*onlineDevice))
	}

	return nil
}

func filterReportTraffic(snapshot []vCore.UserTrafficSnapshot, minKB int) []vCore.UserTrafficSnapshot {
	if len(snapshot) == 0 {
		return nil
	}
	threshold := int64(minKB) * 1000
	result := make([]vCore.UserTrafficSnapshot, 0, len(snapshot))
	for _, t := range snapshot {
		if t.Upload+t.Download > threshold {
			result = append(result, t)
		}
	}
	return result
}

func toPanelTraffic(snapshot []vCore.UserTrafficSnapshot) []panel.UserTraffic {
	result := make([]panel.UserTraffic, len(snapshot))
	for i, t := range snapshot {
		result[i] = panel.UserTraffic{
			UID:      t.UID,
			Upload:   t.Upload,
			Download: t.Download,
		}
	}
	return result
}

// mergeUserList applies the successfully processed user changes to the current
// list. Users whose add/delete failed are intentionally kept so that the next
// poll can retry them.
func mergeUserList(current, added, deleted, modified []panel.UserInfo) []panel.UserInfo {
	next := make(map[string]panel.UserInfo, len(current)+len(added))
	for _, u := range current {
		next[u.Uuid] = u
	}
	for _, u := range deleted {
		delete(next, u.Uuid)
	}
	for _, u := range added {
		next[u.Uuid] = u
	}
	for _, u := range modified {
		next[u.Uuid] = u
	}
	result := make([]panel.UserInfo, 0, len(next))
	for _, u := range next {
		result = append(result, u)
	}
	return result
}

func compareUserList(old, new []panel.UserInfo) (deleted, added, modified []panel.UserInfo) {
	oldMap := make(map[string]panel.UserInfo, len(old))
	for _, u := range old {
		oldMap[u.Uuid] = u
	}

	for _, u := range new {
		if o, ok := oldMap[u.Uuid]; !ok {
			added = append(added, u)
		} else {
			if o.SpeedLimit != u.SpeedLimit || o.DeviceLimit != u.DeviceLimit {
				modified = append(modified, u)
			}
			delete(oldMap, u.Uuid)
		}
	}

	for _, o := range oldMap {
		deleted = append(deleted, o)
	}

	return deleted, added, modified
}
