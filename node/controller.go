package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/task"
	"github.com/wyx2685/v2node/conf"
	"github.com/wyx2685/v2node/core"
	"github.com/wyx2685/v2node/limiter"
)

type Controller struct {
	server                  *core.V2Core
	apiClient               *panel.Client
	tag                     string
	limiter                 *limiter.Limiter
	userList                []panel.UserInfo
	aliveMap                map[int]int
	prefetched              bool
	aliveUnsupported        bool
	conf                    *conf.NodeConfig
	info                    *panel.NodeInfo
	nodeInfoMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	renewCertPeriodic       *task.Task
	reportSem               chan struct{}
}

// NewController return a Node controller with default parameters.
func NewController(api *panel.Client, conf *conf.NodeConfig, info *panel.NodeInfo) *Controller {
	controller := &Controller{
		apiClient: api,
		info:      info,
		conf:      conf,
		reportSem: make(chan struct{}, 1),
	}
	return controller
}

// Start implement the Start() function of the service interface
func (c *Controller) Start(x *core.V2Core) error {
	// Init Core
	c.server = x
	var err error
	// First fetch Node Info
	node := c.info
	if node == nil {
		c.info, err = c.apiClient.GetNodeInfo(context.Background())
		if err != nil {
			return fmt.Errorf("get node info error: %s", err)
		}
		node = c.info
	}
	// Use panel data prefetched by node.New when available. This keeps reload
	// independent from the panel after the old service has been stopped.
	if !c.prefetched {
		c.userList, err = c.apiClient.GetUserList(context.Background())
		if err != nil {
			return fmt.Errorf("get user list error: %s", err)
		}
		c.aliveMap, err = c.apiClient.GetUserAlive(context.Background())
		if err != nil {
			return fmt.Errorf("failed to get user alive list: %s", err)
		}
	}
	if len(c.userList) == 0 {
		return errors.New("add users error: not have any user")
	}
	c.tag = node.Tag

	// add limiter
	l := limiter.AddLimiter(c.info.Type, c.tag, c.info.Common.SpeedLimit, c.userList, c.aliveMap)
	c.limiter = l
	log.WithField("tag", c.tag).Infof("Node speed limit: %d Mbps", c.info.Common.SpeedLimit)
	if node.Security == panel.Tls {
		err = c.requestCert()
		if err != nil {
			return fmt.Errorf("request cert error: %s", err)
		}
	}
	// Add new tag
	err = c.server.AddNode(c.tag, node)
	if err != nil {
		return fmt.Errorf("add new node error: %s", err)
	}
	added, err := c.server.AddUsers(&core.AddUsersParams{
		Tag:      c.tag,
		Users:    c.userList,
		NodeInfo: node,
	})
	if err != nil {
		return fmt.Errorf("add users error: %s", err)
	}
	// Keep only users that are actually present in the core. Users that failed
	// to be added will be retried by the periodic user monitor.
	c.userList = added
	log.WithField("tag", c.tag).Infof("Added %d new users", len(added))
	c.info = node
	c.startTasks(node)
	return nil
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
	// Stop periodic tasks first so they cannot access the limiter or core while
	// the node is being torn down.
	if c.nodeInfoMonitorPeriodic != nil {
		c.nodeInfoMonitorPeriodic.Close()
	}
	if c.userReportPeriodic != nil {
		c.userReportPeriodic.Close()
	}
	if c.renewCertPeriodic != nil {
		c.renewCertPeriodic.Close()
	}

	// Flush the last traffic/online-device window before tearing down the node.
	// reportSem serializes this with an in-flight periodic report.
	if c.server != nil && c.apiClient != nil && c.limiter != nil && c.info != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := c.reportUserTrafficTask(ctx); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Warn("Final traffic report failed")
		} else {
			log.WithField("tag", c.tag).Info("Final traffic report completed")
		}
		cancel()
	}

	limiter.DeleteLimiter(c.tag)
	if c.server == nil {
		return nil
	}
	err := c.server.DelNode(c.tag)
	if err != nil {
		return fmt.Errorf("del node error: %s", err)
	}
	return nil
}
