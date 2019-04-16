# Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

OS := $(shell uname)

# set the environmental variable DRAGONBOAT_LOGDB to lmdb to use lmdb based
# LogDB implementation. 
ifeq ($(DRAGONBOAT_LOGDB),leveldb)
$(info using leveldb based log storage)
GOCMD=go
LOGDB_TAG=dragonboat_leveldb
else ifeq ($(DRAGONBOAT_LOGDB),pebble)
GOCMD=go
LOGDB_TAG=dragonboat_pebble
else ifeq ($(DRAGONBOAT_LOGDB),custom)
$(info using custom lodb)
GOCMD=go
LOGDB_TAG=dragonboat_custom_logdb
else ifeq ($(DRAGONBOAT_LOGDB),)
$(info using rocksdb based log storage)
# set the variables below to tell the Makefile where to find 
# rocksdb libs and includes, e.g. /usr/local/lib and /usr/local/include
# tested rocksdb version -
# rocksdb 5.13.x
# very briefly tested -
# rocksdb 5.15.10
# rocksdb 5.16.6
ROCKSDB_MAJOR_VER=5
ROCKSDB_MINOR_VER=13
ROCKSDB_PATCH_VER=4
ROCKSDB_VER ?= $(ROCKSDB_MAJOR_VER).$(ROCKSDB_MINOR_VER).$(ROCKSDB_PATCH_VER)

ifeq ($(OS),Darwin)
ROCKSDB_SO_FILE=librocksdb.$(ROCKSDB_MAJOR_VER).dylib
else ifeq ($(OS),Linux)
ROCKSDB_SO_FILE=librocksdb.so.$(ROCKSDB_MAJOR_VER)
else
$(error OS type $(OS) not supported)
endif

ROCKSDB_INC_PATH ?=
ROCKSDB_LIB_PATH ?=
# the location of this Makefile
PKGROOT=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
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
endif
endif
endif
endif 

ifeq ($(ROCKSDB_LIB_PATH),)
CDEPS_LDFLAGS=-lrocksdb
else
CDEPS_LDFLAGS=-L$(ROCKSDB_LIB_PATH) -lrocksdb
endif

ifneq ($(ROCKSDB_INC_PATH),)
CGO_CXXFLAGS=CGO_CFLAGS="-I$(ROCKSDB_INC_PATH)"
endif

CGO_LDFLAGS=CGO_LDFLAGS="$(CDEPS_LDFLAGS)"
GOCMD=$(CGO_LDFLAGS) $(CGO_CXXFLAGS) go
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
# set the RACE environmental variable to 1 to enable it, e.g.
# RACE=1 make test-monkey-drummer
ifeq ($(RACE),1)
RACE_DETECTOR_FLAG=-race
$(warning "data race detector enabled, this is a DEBUG build")
endif
# when using dragonboat patched rocksdb, some extra options not exported in
# rocksdb's C interface can be enabled. see available patches in the scripts
# directory for details. note that this is being deprecated. 
ifneq ($(DRAGONBOAT_RDBPATCHED),)
$(info RDB patched enabled, pipelined write and subcompactions are supported)
RDBPATCHED_TAG=dragonboat_rdbpatched
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

INSTALL_PATH ?= /usr/local
PKGNAME=github.com/lni/dragonboat
# shared lib and version number related
LIBNAME=libdragonboat
PLATFORM_SHARED_EXT=so
SHARED_MAJOR=$(shell egrep "DragonboatMajor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_MINOR=$(shell egrep "DragonboatMinor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_PATCH=$(shell egrep "DragonboatPatch = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED1=${LIBNAME}.$(PLATFORM_SHARED_EXT)
SHARED2=$(SHARED1).$(SHARED_MAJOR)
SHARED3=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)

# testing bin
SNAPSHOT_BENCHMARK_TESTING_BIN=snapbench
LOGDB_CHECKER_BIN=logdb-checker-bin
PORCUPINE_CHECKER_BIN=porcupine-checker-bin
DRUMMER_MONKEY_TESTING_BIN=drummer-monkey-testing
DUMMY_TEST_BIN=test.bin
PLUGIN_KVSTORE_BIN=dragonboat-plugin-kvtest.so
PLUGIN_CONCURRENTKV_BIN=dragonboat-plugin-concurrentkv.so
PLUGIN_DATASTORE_BIN=dragonboat-plugin-kvstore.so

ifeq ($(OS),Darwin)
CPPTEST_LDFLAGS=-bundle -undefined dynamic_lookup \
	-Wl,-install_name,$(PLUGIN_CPP_EXAMPLE_BIN)
CPPKVTEST_LDFLAGS=-bundle -undefined dynamic_lookup \
	-Wl,-install_name,$(PLUGIN_CPP_KVTEST_BIN)
else ifeq ($(OS),Linux)
CPPTEST_LDFLAGS=-shared -Wl,-soname,$(PLUGIN_CPP_EXAMPLE_BIN)
CPPKVTEST_LDFLAGS=-shared -Wl,-soname,$(PLUGIN_CPP_KVTEST_BIN)
else
$(error OS type $(OS) not supported)
endif

# go build tags
GOBUILDTAGVALS+=$(LOGDB_TAG)
GOBUILDTAGVALS+=$(RDBPATCHED_TAG)
GOBUILDTAGVALS+=$(ADV_TAG)
GOBUILDTAGS="$(GOBUILDTAGVALS)"
TESTTAGVALS+=$(GOBUILDTAGVALS)
TESTTAGVALS+=$(LOGDB_TEST_BUILDTAGS)
TESTTAGVALS+=$(CPPWRAPPER_TEST_BUILDTAGS)
TESTTAGS="$(TESTTAGVALS)"
BINDINGTAGS="$(GOBUILDTAGVALS) $(BINDING_TAG)"
$(info build tags are set to $(GOBUILDTAGS))
IOERROR_INJECTION_BUILDTAGS=dragonboat_errorinjectiontest
DRUMMER_SLOW_TEST_BUILDTAGS=dragonboat_slowtest $(GOBUILDTAGVALS)
DRUMMER_MONKEY_TEST_BUILDTAGS=dragonboat_monkeytest $(GOBUILDTAGVALS)
MULTIRAFT_SLOW_TEST_BUILDTAGS=dragonboat_slowtest
LOGDB_TEST_BUILDTAGS=dragonboat_logdbtesthelper
GRPC_TEST_BUILDTAGS=dragonboat_grpc_test

all:
rebuild-all: clean all-slow-monkey-tests unit-test-bin
###############################################################################
# download and install rocksdb
###############################################################################
LIBCONF_PATH=/etc/ld.so.conf.d/usr_local_lib.conf
RDBTMPDIR=$(PKGROOT)/build/rocksdbtmp
RDBURL=https://github.com/facebook/rocksdb/archive/v$(ROCKSDB_VER).tar.gz
build-rocksdb: get-rocksdb patch-rocksdb make-rocksdb
build-original-rocksdb : get-rocksdb make-rocksdb
get-rocksdb:
	@{ \
		set -e; \
		rm -rf $(RDBTMPDIR); \
		mkdir -p $(RDBTMPDIR); \
		wget $(RDBURL) -P $(RDBTMPDIR); \
		tar xzvf $(RDBTMPDIR)/v$(ROCKSDB_VER).tar.gz -C $(RDBTMPDIR); \
	}
patch-rocksdb:
	@(cd $(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) \
		&& patch -p1 < $(PKGROOT)/scripts/rocksdb-$(ROCKSDB_VER).patch)
make-rocksdb:
	@make -C $(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) -j16 shared_lib
ldconfig-rocksdb-lib-ull:
	if [ $(OS) = Linux ]; then \
		sudo sh -c "if [ ! -f $(LIBCONF_PATH) ]; \
			then touch $(LIBCONF_PATH); \
			fi"; \
		sudo sh -c "if ! egrep -q '/usr/local/lib' $(LIBCONF_PATH); \
			then echo '/usr/local/lib' >> $(LIBCONF_PATH); \
			fi"; \
		sudo ldconfig; \
  fi
install-rocksdb-lib-ull:
	@{ \
		set -e; \
		sudo INSTALL_PATH=/usr/local make -C \
			$(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) install-shared; \
		rm -rf $(RDBTMPDIR); \
	}
do-install-rocksdb-ull: install-rocksdb-lib-ull ldconfig-rocksdb-lib-ull

do-install-rocksdb-ull-darwin:
	@{ \
		sudo INSTALL_PATH=/usr/local make -C \
			$(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) install-shared; \
		rm -rf $(RDBTMPDIR); \
	}
do-install-rocksdb:
	@(INSTALL_PATH=$(PKGROOT)/build make -C \
		$(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) install-shared && rm -rf $(RDBTMPDIR))

install-rocksdb-ull-darwin: build-rocksdb do-install-rocksdb-ull-darwin
install-rocksdb-ull: build-rocksdb do-install-rocksdb-ull
install-rocksdb: build-rocksdb do-install-rocksdb
install-original-rocksdb-ull: build-original-rocksdb do-install-rocksdb-ull
install-original-rocksdb: build-original-rocksdb do-install-rocksdb

###############################################################################
# builds
###############################################################################
install-dragonboat: gen-gitversion
	$(GO) install $(VERBOSE) -tags=$(GOBUILDTAGS) $(PKGNAME)

gen-gitversion:
	@echo "package dragonboat\n" > gitversion.go
	@echo "const GITVERSION = \"$(shell git rev-parse HEAD)\"" >> gitversion.go

GOBUILD=$(GO) build $(VERBOSE) -tags=$(GOBUILDTAGS) -o $@
$(PLUGIN_KVSTORE_BIN):
	$(GO) build $(RACE_DETECTOR_FLAG) -o $@ $(VERBOSE) -buildmode=plugin \
		$(PKGNAME)/internal/tests/kvtest
plugin-kvtest: $(PLUGIN_KVSTORE_BIN)

$(PLUGIN_CONCURRENTKV_BIN):
	$(GO) build $(RACE_DETECTOR_FLAG) -o $@ $(VERBOSE) -buildmode=plugin \
		$(PKGNAME)/internal/tests/concurrentkv
plugin-concurrentkv: $(PLUGIN_CONCURRENTKV_BIN)

$(DRUMMER_MONKEY_TESTING_BIN):
	$(GO) test $(RACE_DETECTOR_FLAG) $(VERBOSE) \
		-tags="$(DRUMMER_MONKEY_TEST_BUILDTAGS)" -c -o $@ $(PKGNAME)/internal/drummer
drummer-monkey-test-bin: $(DRUMMER_MONKEY_TESTING_BIN)

$(PORCUPINE_CHECKER_BIN):
	$(GO) build -o $@ $(VERBOSE) $(PKGNAME)/internal/tests/lcm/checker
porcupine-checker: $(PORCUPINE_CHECKER_BIN)

$(LOGDB_CHECKER_BIN):
	$(GO) build -o $@ $(VERBOSE) $(PKGNAME)/internal/tests/logdb/checker
logdb-checker: $(LOGDB_CHECKER_BIN)

###############################################################################
# docker tests
###############################################################################
gen-test-docker-images:
	docker build -t dragonboat-ubuntu-test:18.04 -f scripts/Dockerfile-ubuntu-18.04 .
	docker build -t dragonboat-debian-test:9.4 -f scripts/Dockerfile-debian-9.4 .
	docker build -t dragonboat-debian-test:testing -f scripts/Dockerfile-debian-testing .
	docker build -t dragonboat-centos-test:7.5 -f scripts/Dockerfile-centos-7.5 .
	docker build -t dragonboat-go-test:1.9 -f scripts/Dockerfile-go-1.9 .
	docker build -t dragonboat-mindeps-test:1.9 -f scripts/Dockerfile-min-deps .

DOCKERROOTDIR="/go/src/github.com/lni/dragonboat"
DOCKERRUN=docker run --rm -v $(PKGROOT):$(DOCKERROOTDIR)
docker-test: docker-test-ubuntu-stable docker-test-debian-testing \
	docker-test-debian-stable docker-test-centos-stable docker-test-go-old \
	docker-test-min-deps
docker-test-ubuntu-stable: clean
	$(DOCKERRUN) -t dragonboat-ubuntu-test:18.04
docker-test-centos-stable: clean
	$(DOCKERRUN) -t dragonboat-centos-test:7.5
docker-test-debian-testing: clean
	$(DOCKERRUN) -t dragonboat-debian-test:testing
docker-test-debian-stable: clean
	$(DOCKERRUN) -t dragonboat-debian-test:9.4
docker-test-go-old: clean
	$(DOCKERRUN) -t dragonboat-go-test:1.9
docker-test-min-deps: clean
	$(DOCKERRUN) -t dragonboat-mindeps-test:1.9

###############################################################################
# tests
###############################################################################
TEST_OPTIONS=test -tags=$(TESTTAGS) -count=1 $(VERBOSE) \
	$(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION)
BUILD_TEST_ONLY=-c -o test.bin 
dragonboat-test: test-raft test-raftpb test-rsm test-logdb test-transport \
	test-multiraft test-utils test-config test-client test-server test-tests
ci-quick-test: test-raft test-raftpb test-rsm test-logdb test-transport \
  test-utils test-config test-client test-server test-tests
test: dragonboat-test test-drummer
slow-test: test-slow-multiraft test-slow-drummer
more-test: test test-slow-multiraft test-slow-drummer
monkey-test: test-monkey-drummer
dev-test: test test-grpc-transport

###############################################################################
# build unit tests
###############################################################################
unit-test-bin: TEST_OPTIONS=test -c -o $@.bin -tags=$(TESTTAGS) \
	-count=1 $(VERBOSE) $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION) 
unit-test-bin: test-raft test-raftpb test-rsm test-logdb test-transport \
  test-multiraft test-utils test-config test-client test-server

###############################################################################
# fast tests executed for every git push
###############################################################################
benchmark:
	$(GOTEST) $(SELECTED_BENCH_OPTION)

benchmark-fsync:
	$(GOTEST)	-run ^$$ -bench=BenchmarkFSyncLatency

GOTEST=$(GO) $(TEST_OPTIONS)
test-utils:
	$(GOTEST) $(PKGNAME)/internal/utils/syncutil
	$(GOTEST) $(PKGNAME)/internal/utils/netutil
	$(GOTEST) $(PKGNAME)/internal/utils/fileutil
	$(GOTEST) $(PKGNAME)/internal/utils/cache
test-server:
	$(GOTEST) $(PKGNAME)/internal/server
test-config:
	$(GOTEST) $(PKGNAME)/config
test-client:
	$(GOTEST) $(PKGNAME)/client
test-raft:
	$(GOTEST) $(PKGNAME)/internal/raft
test-raftpb:
	$(GOTEST) $(PKGNAME)/raftpb
test-rsm: TESTTAGS=""
test-rsm:
	$(GOTEST) $(PKGNAME)/internal/rsm
test-logdb:
	$(GOTEST) $(PKGNAME)/internal/logdb
test-transport:
	$(GOTEST) $(PKGNAME)/internal/transport
test-grpc-transport: TESTTAGVALS+=$(GRPC_TEST_BUILDTAGS)
test-grpc-transport:
	$(GOTEST) $(PKGNAME)/internal/transport
test-multiraft:
	$(GOTEST) $(PKGNAME)
test-tests:
	$(GOTEST) $(PKGNAME)/internal/tests
test-drummer:
	$(GOTEST) $(PKGNAME)/internal/drummer

###############################################################################
# slow tests excuted nightly & after major changes
###############################################################################

slow-multiraft-ioerror-test-bin: TESTTAGVALS+=$(IOERROR_INJECTION_BUILDTAGS)
slow-multiraft-ioerror-test-bin:
	$(GOTEST) -c -o $(MULTIRAFT_ERROR_INJECTION_TESTING_BIN) $(PKGNAME)

test-slow-multiraft: TESTTAGVALS+=$(MULTIRAFT_SLOW_TEST_BUILDTAGS)
test-slow-multiraft:
	$(GOTEST) $(PKGNAME)

test-slow-drummer: TESTTAGVALS+=$(DRUMMER_SLOW_TEST_BUILDTAGS)
test-slow-drummer: plugin-kvtest
	$(GOTEST) -o $(DRUMMER_MONKEY_TESTING_BIN) -c $(PKGNAME)/internal/drummer
	./$(DRUMMER_MONKEY_TESTING_BIN) -test.v -test.timeout 9999s

test-monkey-drummer: TESTTAGVALS+=$(DRUMMER_MONKEY_TEST_BUILDTAGS)
test-monkey-drummer: plugin-kvtest plugin-concurrentkv
	$(GOTEST) -o $(DRUMMER_MONKEY_TESTING_BIN) -c $(PKGNAME)/internal/drummer
	./$(DRUMMER_MONKEY_TESTING_BIN) -test.v -test.timeout 9999s

###############################################################################
# snapshot benchmark test to check actual bandwidth achieved when streaming
# snapshot images
###############################################################################

snapshot-benchmark-test:
	$(GO) build -v -o $(SNAPSHOT_BENCHMARK_TESTING_BIN) \
		$(PKGNAME)/internal/tests/snapshotbench

###############################################################################
# build slow/monkey tests 
###############################################################################
# build slow and monkey tests, this is invoked for every push
# we can't afford to run all these tests that are known to be slow, but we can
# check whether the push fails the build
slow-multiraft: TESTTAGVALS+=$(MULTIRAFT_SLOW_TEST_BUILDTAGS)
slow-multiraft:
	$(GOTEST) $(BUILD_TEST_ONLY) $(PKGNAME)

slow-drummer: TESTTAGVALS+=$(DRUMMER_SLOW_TEST_BUILDTAGS)
slow-drummer: plugin-kvtest
	$(GOTEST) $(BUILD_TEST_ONLY) $(PKGNAME)/internal/drummer

monkey-drummer: TESTTAGVALS+=$(DRUMMER_MONKEY_TEST_BUILDTAGS)
monkey-drummer: plugin-kvtest
	$(GOTEST) $(BUILD_TEST_ONLY) $(PKGNAME)/internal/drummer

all-slow-monkey-tests: slow-multiraft slow-drummer monkey-drummer

###############################################################################
# static checks
###############################################################################
CHECKED_PKGS=internal/raft internal/logdb internal/transport \
	internal/rsm internal/settings internal/tests \
	internal/tests/lcm internal/utils/lang internal/utils/random \
	internal/utils/fileutil internal/utils/syncutil \
	internal/utils/stringutil internal/utils/logutil internal/utils/netutil \
	internal/utils/cache internal/utils/envutil internal/utils/compression \
	internal/server raftpb \
	logger raftio config statemachine internal/drummer client \
	internal/drummer/client

static-check:
	$(GO) vet -tests=false $(PKGNAME)
	golint $(PKGNAME)
	@for p in $(CHECKED_PKGS); do \
		if [ $$p = "internal/logdb" ] ; \
		then \
			$(GO) vet -tags="$(LOGDB_TEST_BUILDTAGS)" $(PKGNAME)/$$p; \
		else \
			$(GO) vet $(PKGNAME)/$$p; \
		fi; \
		golint $$p; \
		ineffassign $$p; \
		if [ $$p != "internal/drummer" ] ; \
		then \
			gocyclo -over 41 $$p; \
		fi; \
	done;

###############################################################################
# clean
###############################################################################
clean:
	@find . -type d -name "*safe_to_delete" -print | xargs rm -rf
	@rm -f gitversion.go 
	@rm -f test-*.bin
	@rm -f $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4) \
		$(BINDING_DIR)/$(SHARED4) \
		$(BINDING_BIN_HEADER) \
		$(SEQUENCE_TESTING_BIN) \
		$(DUMMY_TEST_BIN) \
		$(IOERROR_INJECTION_BUILDTAGS) \
		$(DRUMMER_MONKEY_TESTING_BIN) \
		$(PLUGIN_KVSTORE_BIN) \
		$(PLUGIN_CONCURRENTKV_BIN) \
		$(PLUGIN_CPP_KVTEST_BIN) \
		$(CPPKVTEST_OBJS) \
		$(WRAPPER_TESTING_BIN) \
		$(CPPTEST_OBJS) \
		$(BINDING_BIN) \
		$(BINDING_OBJS) \
		$(BINDING_STATIC_LIB) \
		$(CPPWRAPPER_TEST_BIN) \
		$(CPPWRAPPER_TEST_OBJS) \
		$(CPPKVTEST_AUTO_GEN_FILES) \
		$(DUMMY_TEST_BIN) \
		$(SNAPSHOT_BENCHMARK_TESTING_BIN) \
		$(PLUGIN_CPP_EXAMPLE_BIN) \
		$(MULTIRAFT_ERROR_INJECTION_TESTING_BIN) \
		$(PORCUPINE_CHECKER_BIN) $(LOGDB_CHECKER_BIN)

.PHONY: gen-gitversion install-dragonboat install-rocksdb \
  drummercmd drummer nodehost \
	$(PLUGIN_CPP_KVTEST_BIN) $(DRUMMER_MONKEY_TESTING_BIN) \
	$(MULTIRAFT_MONKEY_TESTING_BIN) $(PLUGIN_KVSTORE_BIN) $(PLUGIN_CONCURRENTKV_BIN) \
	$(PORCUPINE_CHECKER_BIN) $(LOGDB_CHECKER_BIN) \
	drummer-monkey-test-bin test test-raft test-rsm test-logdb \
	test-transport test-multiraft test-drummer test-session test-server test-utils \
	test-config test-tests static-check clean plugin-kvtest logdb-checker \
	test-monkey-drummer test-slow-multiraft test-grpc-transport \
	test-slow-drummer slow-test more-test monkey-test dev-test \
	slow-multiraft-ioerror-test-bin all-slow-monkey-tests \
	gen-test-docker-images docker-test dragonboat-test snapshot-benchmark-test \
	docker-test-ubuntu-stable docker-test-go-old docker-test-debian-testing \
	docker-test-debian-stable docker-test-centos-stable docker-test-min-deps
