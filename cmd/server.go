package cmd

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/wyx2685/v2node/conf"
	"github.com/wyx2685/v2node/core"
	"github.com/wyx2685/v2node/limiter"
	"github.com/wyx2685/v2node/node"
)

var (
	config string
)

var serverCommand = cobra.Command{
	Use:   "server",
	Short: "Run v2node server",
	Run:   serverHandle,
	Args:  cobra.NoArgs,
}

func init() {
	serverCommand.PersistentFlags().
		StringVarP(&config, "config", "c",
			"/etc/v2node/config.json", "config file path")
	command.AddCommand(&serverCommand)
}

func serverHandle(_ *cobra.Command, _ []string) {
	showVersion()
	c := conf.New()
	err := c.LoadFromPath(config)
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: true,
		DisableQuote:     true,
		PadLevelText:     false,
	})
	if err != nil {
		log.WithField("err", err).Error("Load config file failed")
		return
	}
	switch c.LogConfig.Level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	}
	if c.LogConfig.Output != "" {
		f, err := os.OpenFile(c.LogConfig.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.WithField("err", err).Error("Open log file failed, using stdout instead")
		} else {
			log.SetOutput(f)
		}
	}
	defer func() {
		if f, ok := log.StandardLogger().Out.(*os.File); ok && f != os.Stdout && f != os.Stderr {
			_ = f.Close()
		}
	}()
	// Enable pprof if configured
	if c.PprofPort != 0 {
		go func() {
			log.Infof("Starting pprof server on :%d", c.PprofPort)
			if err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", c.PprofPort), nil); err != nil {
				log.WithField("err", err).Error("pprof server failed")
			}
		}()
	}
	//init limiter
	limiter.Init()
	//get node info
	nodes, err := node.New(c.NodeConfigs)
	if err != nil {
		log.WithField("err", err).Error("Get node info failed")
		return
	}
	log.Info("Got nodes info from server")
	//core
	var reloadCh = make(chan struct{}, 1)
	v2core := core.New(c)
	v2core.ReloadCh = reloadCh
	err = v2core.Start(nodes.NodeInfos)
	if err != nil {
		log.WithField("err", err).Error("Start core failed")
		return
	}
	defer func() {
		if err := v2core.Close(); err != nil {
			log.WithField("err", err).Error("Close core failed")
		}
	}()
	//node
	err = nodes.Start(c.NodeConfigs, v2core)
	if err != nil {
		log.WithField("err", err).Error("Run nodes failed")
		return
	}
	log.Info("Nodes started")
	defer func() {
		if err := nodes.Close(); err != nil {
			log.WithField("err", err).Error("Close nodes failed")
		}
	}()
	// clear memory
	runtime.GC()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-osSignals:
			log.Info("收到退出信号，正在关闭程序...")
			return
		case <-reloadCh:
			log.Info("收到重启信号，正在重新加载配置...")
			if err := reload(config, &nodes, &v2core); err != nil {
				log.WithField("err", err).Error("reload failed")
			} else {
				log.Info("重启成功")
			}
		}
	}
}

func reload(config string, nodes **node.Node, v2core **core.V2Core) error {
	var oldConf *conf.Conf
	var oldReloadCh chan struct{}
	if *v2core != nil {
		oldConf = (*v2core).Config
		oldReloadCh = (*v2core).ReloadCh
	}

	// Load and validate the new configuration before touching the old service.
	newConf := conf.New()
	if err := newConf.LoadFromPath(config); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Open the new log file before stopping the old service.
	newLogFile, err := openLogFile(newConf.LogConfig.Output)
	if err != nil {
		return err
	}
	logCommitted := false
	defer func() {
		if !logCommitted && newLogFile != nil {
			_ = newLogFile.Close()
		}
	}()

	// Fetch panel data for the new configuration while the old service is
	// still running.
	newNodes, err := node.New(newConf.NodeConfigs)
	if err != nil {
		return fmt.Errorf("get node info: %w", err)
	}

	// The old service can be stopped only after the new configuration and
	// panel data have been validated.
	if err := (*nodes).Close(); err != nil {
		log.WithField("err", err).Error("Close old nodes failed")
	}
	if err := (*v2core).Close(); err != nil {
		log.WithField("err", err).Error("Close old core failed")
	}

	newCore, err := startCore(newConf, newNodes, oldReloadCh)
	if err != nil {
		// Try to restore the old configuration if starting the new one failed.
		if oldConf != nil {
			if rbNodes, rbCore, rbErr := startNodesAndCore(oldConf, oldReloadCh); rbErr == nil {
				*nodes = rbNodes
				*v2core = rbCore
				return fmt.Errorf("start new service: %w; rolled back to old config", err)
			} else {
				return fmt.Errorf("start new service: %w; rollback failed: %v", err, rbErr)
			}
		}
		return fmt.Errorf("start new service: %w", err)
	}

	// Commit log settings and swap instances only after the new service started.
	applyLogLevel(newConf.LogConfig.Level)
	switchLogOutput(newLogFile)
	logCommitted = true

	*nodes = newNodes
	*v2core = newCore

	runtime.GC()
	return nil
}

func startCore(c *conf.Conf, n *node.Node, reloadCh chan struct{}) (*core.V2Core, error) {
	v := core.New(c)
	v.ReloadCh = reloadCh
	if err := v.Start(n.NodeInfos); err != nil {
		_ = v.Close()
		_ = n.Close()
		return nil, fmt.Errorf("start core: %w", err)
	}
	if err := n.Start(c.NodeConfigs, v); err != nil {
		_ = v.Close()
		_ = n.Close()
		return nil, fmt.Errorf("start nodes: %w", err)
	}
	return v, nil
}

func startNodesAndCore(c *conf.Conf, reloadCh chan struct{}) (*node.Node, *core.V2Core, error) {
	n, err := node.New(c.NodeConfigs)
	if err != nil {
		return nil, nil, err
	}
	v, err := startCore(c, n, reloadCh)
	if err != nil {
		return nil, nil, err
	}
	return n, v, nil
}

func openLogFile(path string) (*os.File, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	return f, nil
}

func applyLogLevel(level string) {
	switch level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	}
}

func switchLogOutput(f *os.File) {
	old := log.StandardLogger().Out
	if f != nil {
		log.SetOutput(f)
	} else {
		log.SetOutput(os.Stderr)
	}
	if oldFile, ok := old.(*os.File); ok && oldFile != os.Stdout && oldFile != os.Stderr {
		_ = oldFile.Close()
	}
}
