// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package logger

import (
	"github.com/lni/dragonboat/v3/internal/utils/logutil/capnslog"
)

const (
	// RepoName is the repo name used in capnslog.
	RepoName = "github.com/lni/dragonboat/v3"
)

// CreateCapnsLog creates an ILogger instance based on capnslog.
func CreateCapnsLog(pkgName string) ILogger {
	c := &capnsLog{
		logger: capnslog.NewPackageLogger(RepoName, pkgName),
	}
	return c
}

type capnsLog struct {
	logger *capnslog.PackageLogger
}

func (c *capnsLog) SetLevel(level LogLevel) {
	var cl capnslog.LogLevel
	if level == CRITICAL {
		cl = capnslog.CRITICAL
	} else if level == ERROR {
		cl = capnslog.ERROR
	} else if level == WARNING {
		cl = capnslog.WARNING
	} else if level == INFO {
		cl = capnslog.INFO
	} else if level == DEBUG {
		cl = capnslog.DEBUG
	} else {
		panic("unexpected level")
	}
	c.logger.SetLevel(cl)
}

func (c *capnsLog) Debugf(format string, args ...interface{}) {
	c.logger.Debugf(format, args...)
}

func (c *capnsLog) Infof(format string, args ...interface{}) {
	c.logger.Infof(format, args...)
}

func (c *capnsLog) Warningf(format string, args ...interface{}) {
	c.logger.Warningf(format, args...)
}

func (c *capnsLog) Errorf(format string, args ...interface{}) {
	c.logger.Errorf(format, args...)
}

func (c *capnsLog) Panicf(format string, args ...interface{}) {
	c.logger.Panicf(format, args...)
}
