# Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GOEXEC ?= go
# Dragonboat is known to work on - 
# Linux AMD64, Linux ARM64, MacOS, Windows/MinGW and FreeBSD AMD64
# only Linux AMD64 is officially supported
OS := $(shell uname)
# the location of this Makefile
PKGROOT=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
# name of the package
PKGNAME=github.com/lni/dragonboat/v3
# set the environmental variable DRAGONBOAT_LOGDB to lmdb to use lmdb based
# LogDB implementation. 
ifeq ($(DRAGONBOAT_LOGDB),rocksdb)
GOCMD=$(GOEXEC)
LOGDB_TAG=dragonboat_rocksdb_test
$(info using rocksdb based log storage)
ifeq ($(OS),Darwin)
ROCKSDB_SO_FILE=librocksdb.dylib
else ifeq ($(OS),Linux)
ROCKSDB_SO_FILE=librocksdb.so
else ifeq ($(OS),FreeBSD)
ROCKSDB_SO_FILE=librocksdb.so
else ifneq (,$(findstring MINGW,$(OS)))
$(info running on Windows/MinGW)
else ifneq (,$(findstring MSYS, $(OS)))
$(info running on Windows/MSYS)
else
$(error OS type $(OS) not supported)
endif ## ifeq ($(OS),Darwin)

# RocksDB version 5 or 6 are required
ROCKSDB_INC_PATH ?=
ROCKSDB_LIB_PATH ?=
# figure out where is the rocksdb installation
# supported gorocksdb version in ./build/lib?
ifeq ($(ROCKSDB_LIB_PATH),)
ifeq ($(ROCKSDB_INC_PATH),)
ifneq ($(wildcard $(PKGROOT)/build/lib/$(ROCKSDB_SO_FILE)),)
ifneq ($(wildcard $(PKGROOT)/build/include/rocksdb/c.h),)
$(info rocksdb lib found at $(PKGROOT)/build/lib/$(ROCKSDB_SO_FILE))
ROCKSDB_LIB_PATH=$(PKGROOT)/build/lib
ROCKSDB_INC_PATH=$(PKGROOT)/build/include
endif
endif
endif
endif

# in /usr/local/lib?
ifeq ($(ROCKSDB_LIB_PATH),)
ifeq ($(ROCKSDB_INC_PATH),)
ifneq ($(wildcard /usr/local/lib/$(ROCKSDB_SO_FILE)),)
ifneq ($(wildcard /usr/local/include/rocksdb/c.h),)
$(info rocksdb lib found at /usr/local/lib/$(ROCKSDB_SO_FILE))
ROCKSDB_LIB_PATH=/usr/local/lib
ROCKSDB_INC_PATH=/usr/local/include
endif
endif
endif
endif 

ifeq ($(OS),Linux)
ROCKSDB_LIB_FLAG=-lrocksdb -ldl
else
ROCKSDB_LIB_FLAG=-lrocksdb
endif

# by default, shared rocksdb lib is used. when using the static rocksdb lib,
# you may need to add
# -lbz2 -lsnappy -lz -llz4 
ifeq ($(ROCKSDB_LIB_PATH),)
CDEPS_LDFLAGS=-lrocksdb
else
CDEPS_LDFLAGS=-L$(ROCKSDB_LIB_PATH) -lrocksdb
endif

ifneq ($(ROCKSDB_INC_PATH),)
CGO_CFLAGS=CGO_CFLAGS="-I$(ROCKSDB_INC_PATH)"
endif

CGO_LDFLAGS=CGO_LDFLAGS="$(CDEPS_LDFLAGS)"
GOCMD=$(CGO_LDFLAGS) $(CGO_CFLAGS) $(GOEXEC)

## ifeq ($(DRAGONBOAT_LOGDB),rocksdb)
else ifeq ($(DRAGONBOAT_LOGDB),pebble_memfs)
$(error invalid DRAGONBOAT_LOGDB pebble_memfs)
else ifeq ($(DRAGONBOAT_LOGDB),custom)
$(info using custom lodb)
GOCMD=$(GOEXEC)
else ifeq ($(DRAGONBOAT_LOGDB),)
GOCMD=$(GOEXEC)
ifneq ($(DRAGONBOAT_MEMFS_TEST),)
$(info using memfs based pebble)
LOGDB_TAG=dragonboat_memfs_test
else
$(info using pebble based log storage)
endif

else
$(error LOGDB type $(DRAGONBOAT_LOGDB) not supported)
endif

# verbosity, use -v to see details of go build
VERBOSE ?= -v
ifeq ($(VERBOSE),)
GO=@$(GOCMD)
else
GO=$(GOCMD)
endif
# golang race detector
# set the RACE environmental variable to 1 to enable it, e.g. RACE=1 make test
ifeq ($(RACE),1)
RACE_DETECTOR_FLAG=-race
$(warning "data race detector enabled, this is a DEBUG build")
endif

ifneq ($(TEST_TO_RUN),)
$(info Running selected tests $(TEST_TO_RUN))
SELECTED_TEST_OPTION=-run $(TEST_TO_RUN)
endif

ifneq ($(BENCHMARK_TO_RUN),)
$(info Running selected benchmarks $(BENCHMARK_TO_RUN))
SELECTED_BENCH_OPTION=-run ^$$ -bench=$(BENCHMARK_TO_RUN)
else
SELECTED_BENCH_OPTION=-run ^$$ -bench=.
endif

# go build tags
GOBUILDTAGVALS+=$(LOGDB_TAG)
GOBUILDTAGS="$(GOBUILDTAGVALS)"
TESTTAGVALS+=$(GOBUILDTAGVALS)
TESTTAGS="$(TESTTAGVALS)"
EXTNAME=linux

.PHONY: all
all: unit-test-bin
.PHONY: rebuild-all
rebuild-all: clean unit-test-bin

###############################################################################
# tests
###############################################################################
ifneq ($(TESTTAGS),"")
GOCMDTAGS=-tags=$(TESTTAGS)
endif

TEST_OPTIONS=test $(GOCMDTAGS) -timeout=1200s -count=1 $(VERBOSE) \
  $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION)
.PHONY: dragonboat-test
dragonboat-test: test-raft test-raftpb test-rsm test-logdb test-transport    \
	test-multiraft test-config test-client test-server test-tools test-fs   	 \
	test-utils
.PHONY: travis-ci-test
travis-ci-test: test-raft test-raftpb test-rsm test-logdb test-transport 		 \
  test-config test-client test-server test-tests test-tools test-fs 				 \
	test-utils
.PHONY: test
test: dragonboat-test test-tests
.PHONY: dev-test
dev-test: test test-plugins
.PHONY: travis-test
travis-test: travis-ci-test test-cov

###############################################################################
# build unit tests
###############################################################################
.PHONY: unit-test-bin
unit-test-bin: TEST_OPTIONS=test -c -o $@.bin -tags=$(TESTTAGS) 						 \
	-count=1 $(VERBOSE) $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION) 
.PHONY: unit-test-bin
unit-test-bin: test-raft test-raftpb test-rsm test-logdb test-transport 		 \
  test-multiraft test-config test-client test-server test-tools test-plugins \
	test-tests test-fs test-utils

###############################################################################
# fast tests executed for every git push
###############################################################################
.PHONY: benchmark
benchmark:
	$(GOTEST) $(SELECTED_BENCH_OPTION)

.PHONY: benchmark-fsync
benchmark-fsync:
	$(GOTEST)	-run ^$$ -bench=BenchmarkFSyncLatency

GOTEST=$(GO) $(TEST_OPTIONS)
.PHONY: test-plugins
test-plugins:
	$(GOTEST) $(PKGNAME)/plugin
.PHONY: test-server
test-server:
	$(GOTEST) $(PKGNAME)/internal/server
.PHONY: test-config
test-config:
	$(GOTEST) $(PKGNAME)/config
.PHONY: test-client
test-client:
	$(GOTEST) $(PKGNAME)/client
.PHONY: test-raft
test-raft:
	$(GOTEST) $(PKGNAME)/internal/raft
.PHONY: test-raftpb
test-raftpb:
	$(GOTEST) $(PKGNAME)/raftpb
.PHONY: test-rsm
test-rsm:
	$(GOTEST) $(PKGNAME)/internal/rsm
.PHONY: test-logdb
test-logdb:
	$(GOTEST) $(PKGNAME)/internal/logdb
.PHONY: test-transport
test-transport:
	$(GOTEST) $(PKGNAME)/internal/transport
.PHONY: test-multiraft
test-multiraft:
	$(GOTEST) $(PKGNAME)
.PHONY: test-tests
test-tests:
	$(GOTEST) $(PKGNAME)/internal/tests
.PHONY: test-fs
test-fs:
	$(GOTEST) $(PKGNAME)/internal/fileutil
.PHONY: test-tools
test-tools:
	$(GOTEST) $(PKGNAME)/tools
.PHONY: test-utils
test-utils:
	$(GOTEST) $(PKGNAME)/internal/utils/dio
.PHONY: test-cov
test-cov:
	$(GOTEST) -coverprofile=coverage.txt -covermode=atomic

###############################################################################
# static checks
###############################################################################
CHECKED_PKGS=internal/raft internal/logdb internal/logdb/kv internal/transport \
	internal/vfs internal/rsm internal/settings internal/tests internal/server   \
	internal/logdb/kv/pebble plugin/chan raftpb tools logger raftio config       \
	statemachine client internal/utils/dio

.PHONY: static-check
static-check:
	$(GO) vet -tests=false $(PKGNAME)
	golint $(PKGNAME)
	@for p in $(CHECKED_PKGS); do \
		$(GO) vet $(PKGNAME)/$$p; \
		golint $$p; \
		ineffassign $$p; \
	done;

GOLANGCI_LINT_PKGS=internal/raft internal/rsm internal/vfs internal/transport  \
	internal/server statemachine tools raftpb raftio client tools logger config  \
	internal/logdb/kv/pebble plugin/chan internal/settings internal/tests        \
	internal/logdb/kv internal/utils/dio internal/logdb
EXTRA_LINTERS=-E dupl -E misspell -E scopelint -E interfacer

.PHONY: golangci-lint-check
golangci-lint-check:
	@for p in $(GOLANGCI_LINT_PKGS); do \
		golangci-lint run $$p; \
	done;
	@golangci-lint run $(EXTRA_LINTERS) .

###############################################################################
# clean
###############################################################################
.PHONY: clean
clean:
	@find . -type d -name "*safe_to_delete" -print | xargs rm -rf
	@rm -f gitversion.go 
	@rm -f test-*.*
	@$(GO) clean -i -testcache $(PKG)
