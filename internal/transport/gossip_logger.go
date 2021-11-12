package transport

import (
	"log"
	"strings"

	"github.com/lni/dragonboat/v3/logger"
)

type gossipLogWriter struct {
	logger logger.ILogger
}

func (l *gossipLogWriter) Write(p []byte) (int, error) {
	str := string(p)
	str = strings.TrimSuffix(str, "\n")

	switch {
	case strings.HasPrefix(str, "[WARN] "):
		str = strings.TrimPrefix(str, "[WARN] ")
		l.logger.Warningf(str)
	case strings.HasPrefix(str, "[DEBUG] "):
		str = strings.TrimPrefix(str, "[DEBUG] ")
		l.logger.Debugf(str)
	case strings.HasPrefix(str, "[INFO] "):
		str = strings.TrimPrefix(str, "[INFO] ")
		l.logger.Infof(str)
	case strings.HasPrefix(str, "[ERR] "):
		str = strings.TrimPrefix(str, "[ERR] ")
		l.logger.Warningf(str)
	default:
		l.logger.Warningf(str)
	}

	return len(p), nil
}

// newGossipLogWrapper prepare log wrapper for gossip.
// Inspirited by https://github.com/docker/docker-ce/blob/master/components/engine/libnetwork/networkdb/cluster.go#L30
func newGossipLogWrapper() *log.Logger {
	return log.New(&gossipLogWriter{
		logger: logger.GetLogger("gossip"),
	}, "", 0)
}
