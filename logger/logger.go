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

/*
Package logger manages loggers used in dragonboat.
*/
package logger

import (
	"sync"
)

// LogLevel is the log level defined in dragonboat.
type LogLevel int

const (
	// CRITICAL is the CRITICAL log level
	CRITICAL LogLevel = iota - 1
	// ERROR is the ERROR log level
	ERROR
	// WARNING is the WARNING log level
	WARNING
	// INFO is the INFO log level
	INFO
	// DEBUG is the DEBUG log level
	DEBUG
)

// Factory is the factory method for creating logger used for the
// specified package.
type Factory func(pkgName string) ILogger

// ILogger is the interface implemented by loggers that can be used by
// dragonboat. You can implement your own ILogger implementation by building
// wrapper struct on top of your favourite logging library.
type ILogger interface {
	SetLevel(LogLevel)
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
}

// SetLoggerFactory sets the factory function used to create ILogger instances.
func SetLoggerFactory(f Factory) {
	_loggers.mu.Lock()
	defer _loggers.mu.Unlock()
	if _loggers.loggerFactory != nil {
		panic("setting the logger factory again")
	}
	_loggers.loggerFactory = f
}

// GetLogger returns the logger for the specified package name. The most common
// use case for the returned logger is to set its log verbosity level.
func GetLogger(pkgName string) ILogger {
	_loggers.mu.Lock()
	defer _loggers.mu.Unlock()
	l, ok := _loggers.loggers[pkgName]
	if !ok {
		l = &dragonboatLogger{pkgName: pkgName}
		_loggers.loggers[pkgName] = l
	}
	return l
}

type dragonboatLogger struct {
	mu      sync.Mutex
	logger  ILogger
	pkgName string
}

func (d *dragonboatLogger) createILogger() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.logger == nil {
		d.logger = _loggers.createILogger(d.pkgName)
	}
}

func (d *dragonboatLogger) SetLevel(l LogLevel) {
	d.createILogger()
	d.logger.SetLevel(l)
}

func (d *dragonboatLogger) Debugf(format string, args ...interface{}) {
	d.createILogger()
	d.logger.Debugf(format, args...)
}

func (d *dragonboatLogger) Infof(format string, args ...interface{}) {
	d.createILogger()
	d.logger.Infof(format, args...)
}

func (d *dragonboatLogger) Warningf(format string, args ...interface{}) {
	d.createILogger()
	d.logger.Warningf(format, args...)
}

func (d *dragonboatLogger) Errorf(format string, args ...interface{}) {
	d.createILogger()
	d.logger.Errorf(format, args...)
}

func (d *dragonboatLogger) Panicf(format string, args ...interface{}) {
	d.createILogger()
	d.logger.Panicf(format, args...)
}

type sysLoggers struct {
	mu            sync.Mutex
	loggers       map[string]*dragonboatLogger
	loggerFactory Factory
}

func (l *sysLoggers) createILogger(pkgName string) ILogger {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.loggerFactory == nil {
		return createDefaultILogger(pkgName)
	}
	return l.loggerFactory(pkgName)
}

var _loggers = createSysLoggers()

func createSysLoggers() *sysLoggers {
	s := &sysLoggers{
		loggers: make(map[string]*dragonboatLogger),
	}
	return s
}

func createDefaultILogger(pkgName string) ILogger {
	return CreateCapnsLog(pkgName)
}
