// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package registry

import (
	"log"
	"strings"

	"github.com/lni/dragonboat/v4/logger"
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
// Inspirited by https://github.com/docker/docker-ce/blob/master/components/engine/libnetwork/networkdb/shard.go#L30
func newGossipLogWrapper() *log.Logger {
	return log.New(&gossipLogWriter{
		logger: logger.GetLogger("gossip"),
	}, "", 0)
}
