# Copyright 2018-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

ifeq ($(DRAGONBOAT_LOGDB),rocksdb)
LOGDB_TAG=dragonboat_rocksdb_test
$(info using rocksdb based log storage)
else ifeq ($(DRAGONBOAT_LOGDB),)
ifneq ($(MEMFS_TEST),)
$(info using memfs based pebble)
LOGDB_TAG=dragonboat_memfs_test
else
ifneq ($(MEMFS_TEST_TO_RUN),)
$(info using memfs based pebble)
LOGDB_TAG=dragonboat_memfs_test
else
$(info using pebble based log storage)
endif
endif
else
$(error LOGDB type $(DRAGONBOAT_LOGDB) not supported)
endif

# verbosity, use -v to see details of go build
VERBOSE ?= -v
ifeq ($(VERBOSE),)
GO=@$(GOEXEC)
else
GO=$(GOEXEC)
endif

ifeq ($(RACE),1)
RACE_DETECTOR_FLAG=-race
$(warning "data race detector enabled")
endif

ifeq ($(COVER),1)
COVER_FLAG=-coverprofile=coverage.out
$(warning "coverage enabled, `go tool cover -html=coverage.out` to see results")
endif

ifneq ($(TEST_TO_RUN),)
$(info Running selected tests $(TEST_TO_RUN))
SELECTED_TEST_OPTION=-run $(TEST_TO_RUN)
endif

ifneq ($(MEMFS_TEST_TO_RUN),)
$(info Running selected tests $(MEMFS_TEST_TO_RUN))
SELECTED_TEST_OPTION=-run $(MEMFS_TEST_TO_RUN)
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

TEST_OPTIONS=test $(GOCMDTAGS) -timeout=2400s -count=1 $(VERBOSE) \
  $(RACE_DETECTOR_FLAG) $(COVER_FLAG) $(SELECTED_TEST_OPTION)
.PHONY: dragonboat-test
dragonboat-test: test-raft test-raftpb test-rsm test-logdb test-transport    \
	test-multiraft test-config test-client test-server test-tools test-fs   	 \
	test-id test-utils
.PHONY: ci-test
ci-test: test-raft test-raftpb test-rsm test-logdb test-transport 		       \
  test-config test-client test-server test-tests test-tools test-fs 				 \
	test-id test-utils
.PHONY: test
test: dragonboat-test test-tests
.PHONY: dev-test
dev-test: test test-plugins
.PHONY: actions-test
actions-test: ci-test test-cov

###############################################################################
# build unit tests
###############################################################################
.PHONY: unit-test-bin
unit-test-bin: TEST_OPTIONS=test -c -o $@.bin -tags=$(TESTTAGS) 						 \
	-count=1 $(VERBOSE) $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION) 
.PHONY: unit-test-bin
unit-test-bin: test-raft test-raftpb test-rsm test-logdb test-transport 		 \
  test-multiraft test-config test-client test-server test-tools test-plugins \
	test-tests test-fs test-id test-utils

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
.PHONY: test-id
test-id:
	$(GOTEST) $(PKGNAME)/internal/id
.PHONY: test-utils
test-utils:
	$(GOTEST) $(PKGNAME)/internal/utils/dio
.PHONY: test-cov
test-cov:
	$(GOTEST) -coverprofile=coverage.txt -covermode=atomic

###############################################################################
# tools
###############################################################################
.PHONY: tools
tools: tools-checkdisk

.PHONY: tools-checkdisk
tools-checkdisk:
	$(GO) build $(PKGNAME)/tools/checkdisk

###############################################################################
# static checks
###############################################################################
CHECKED_PKGS=$(shell go list ./...)
CHECKED_DIRS=$(subst $(PKGNAME), ,$(subst $(PKGNAME)/, ,$(CHECKED_PKGS))) .
EXTRA_LINTERS=-E misspell -E scopelint -E interfacer -E rowserrcheck \
	-E depguard -E unconvert -E prealloc -E gofmt -E stylecheck
.PHONY: static-check
static-check:
	@for p in $(CHECKED_PKGS); do \
		go vet -tests=false $$p; \
		golint $$p; \
	done;
	@for p in $(CHECKED_DIRS); do \
		ineffassign $$p; \
		golangci-lint run $(EXTRA_LINTERS) $$p; \
	done;

# -E dupl is not included in regular static check as there are duplicated code
# in auto generated code
.PHONY: extra-static-check
extra-static-check: override EXTRA_LINTERS :=-E dupl
extra-static-check:
	for p in $(CHECKED_DIRS); do \
    golangci-lint run $(EXTRA_LINTERS) $$p; \
  done;

###############################################################################
# clean
###############################################################################
.PHONY: clean
clean:
	@find . -type d -name "*safe_to_delete" -print | xargs rm -rf
	@rm -f gitversion.go 
	@rm -f test-*.*
	@rm -f checkdisk
	@$(GO) clean -i -testcache $(PKG)
