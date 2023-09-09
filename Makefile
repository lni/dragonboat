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
PKGNAME=$(shell go list)

ifeq ($(DRAGONBOAT_LOGDB),rocksdb)
$(error rocksdb is no longer supported)
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
	test-id test-utils test-tan test-registry
.PHONY: ci-test
ci-test: test-raft test-raftpb test-rsm test-logdb test-transport 		       \
  test-config test-client test-server test-tests test-tools test-fs 				 \
	test-id test-utils test-tan test-registry
.PHONY: test
test: dragonboat-test test-tests
.PHONY: dev-test
dev-test: test
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
  test-multiraft test-config test-client test-server test-tools \
	test-tests test-fs test-id test-utils test-tan test-registry

###############################################################################
# fast tests executed for every git push
###############################################################################
.PHONY: benchmark
benchmark:
	$(GOTEST) $(SELECTED_BENCH_OPTION)
.PHONY: benchmark-tan
benchmark-tan:
	$(GOTEST) $(SELECTED_BENCH_OPTION) $(PKGNAME)/internal/tan
.PHONY: benchmark-fsync
benchmark-fsync:
	$(GOTEST)	-run ^$$ -bench=BenchmarkFSyncLatency

GOTEST=$(GO) $(TEST_OPTIONS)
.PHONY: slow-test
slow-test:
	SLOW_TEST=1 $(GOTEST) $(PKGNAME)
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
.PHONY: test-tan
test-tan:
	$(GOTEST) $(PKGNAME)/internal/tan
.PHONY: test-registry
test-registry:
	$(GOTEST) $(PKGNAME)/internal/registry
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
.PHONY: install-static-check-tools
install-static-check-tools:
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.53.2

CHECKED_PKGS=$(shell go list ./...)
CHECKED_DIRS=$(subst $(PKGNAME), ,$(subst $(PKGNAME)/, ,$(CHECKED_PKGS))) .
EXTRA_LINTERS=-E misspell -E rowserrcheck -E unconvert \
	-E prealloc -E stylecheck
.PHONY: static-check
static-check:
	@for p in $(CHECKED_DIRS); do \
		golangci-lint run $(EXTRA_LINTERS) $$p; \
	done;

extra-static-check: override EXTRA_LINTERS :=-E dupl
extra-static-check: static-check

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
