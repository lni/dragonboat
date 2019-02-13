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
# whether to force using libc++ and -std=c++2a
ifneq ($(CXX2A),)
$(info using -std=c++2a -stdlib=libc++)
CXXSTD=-std=c++2a -stdlib=libc++
else
CXXSTD=-std=c++11
endif

ifeq ($(MAKECMDGOALS),dragonboat-cppwrapper-tests)
TESTBUILD=1
endif
ifeq ($(MAKECMDGOALS),test-cppwrapper)
TESTBUILD=1
endif
ifneq ($(TESTBUILD),)
$(info TESTBUILD flag set, doing a DEBUG build)
# TODO:
# re-enable the following option when g++ 7 is fixed in ubuntu 16.04
# https://stackoverflow.com/questions/50024731/ld-unrecognized-option-push-state-no-as-needed
# -fsanitize=undefined
# note that -fsanitize=thread is not used here because we can't have the
# dragonboat library in Go instrumented. 
# https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual
ifeq ($(OS),Darwin)
SANITIZER_FLAGS ?= -fsanitize=address -fsanitize=undefined
else ifeq ($(OS),Linux)
SANITIZER_FLAGS ?= -fsanitize=address -fsanitize=leak
else
$(error OS type $(OS) not supported)
endif
BINDING_STATIC_LIB_TEST_CXXFLAGS=-g $(SANITIZER_FLAGS)
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
BINDING_DIR=binding
BINDING_TAG=dragonboat_language_binding
SHARED_MAJOR=$(shell egrep "DragonboatMajor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_MINOR=$(shell egrep "DragonboatMinor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_PATCH=$(shell egrep "DragonboatPatch = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED1=${LIBNAME}.$(PLATFORM_SHARED_EXT)
SHARED2=$(SHARED1).$(SHARED_MAJOR)
SHARED3=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)

# tests for the cpp wrapper
CFLAGS=-Wall -fPIC -O3 -Ibinding/include
CXXFLAGS=$(CXXSTD) -Wall -fPIC -O3 -Ibinding/include
TEST_CXXFLAGS=$(CXXFLAGS) -g $(SANITIZER_FLAGS)
TEST_LDFLAGS=$(SANITIZER_FLAGS)
# testing bin
SNAPSHOT_BENCHMARK_TESTING_BIN=snapbench
LOGDB_CHECKER_BIN=logdb-checker-bin
PORCUPINE_CHECKER_BIN=porcupine-checker-bin
DRUMMER_MONKEY_TESTING_BIN=drummer-monkey-testing
WRAPPER_TESTING_BIN=cpp-wrapper-testing
PLUGIN_CPP_EXAMPLE_BIN=dragonboat-cpp-plugin-example.so
DUMMY_TEST_BIN=test.bin
PLUGIN_KVSTORE_BIN=dragonboat-plugin-kvtest.so
PLUGIN_CPP_KVTEST_BIN=dragonboat-cpp-plugin-cppkvtest.so
# example bin
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
CPPWRAPPER_TEST_BUILDTAGS=dragonboat_cppwrappertest
GRPC_TEST_BUILDTAGS=dragonboat_grpc_test

all: binding
rebuild-all: clean servers binding all-slow-monkey-tests unit-test-bin
servers: drummer nodehost drummercmd
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

DRUMMER_SERVER_BIN=dragonboat-drummer-server
DRUMMER_CMD_BIN=dragonboat-drummer-cmd
NODEHOST_SERVER_BIN=dragonboat-nodehost-server
GOBUILD=$(GO) build $(VERBOSE) -tags=$(GOBUILDTAGS) -o $@
$(DRUMMER_SERVER_BIN):
	$(GOBUILD) $(PKGNAME)/drummer/server/drummer
drummer: $(DRUMMER_SERVER_BIN)

$(NODEHOST_SERVER_BIN):
	$(GOBUILD) $(PKGNAME)/drummer/server/nodehost
nodehost: $(NODEHOST_SERVER_BIN)

$(DRUMMER_CMD_BIN):
	$(GOBUILD) $(PKGNAME)/drummer/server/drummercmd
drummercmd: $(DRUMMER_CMD_BIN)

$(PLUGIN_KVSTORE_BIN):
	$(GO) build $(RACE_DETECTOR_FLAG) -o $@ $(VERBOSE) -buildmode=plugin \
		$(PKGNAME)/internal/tests/kvtest
plugin-kvtest: $(PLUGIN_KVSTORE_BIN)

$(DRUMMER_MONKEY_TESTING_BIN):
	$(GO) test $(RACE_DETECTOR_FLAG) $(VERBOSE) \
		-tags="$(DRUMMER_MONKEY_TEST_BUILDTAGS)" -c -o $@ $(PKGNAME)/drummer
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
	test-multiraft test-utils test-wrapper test-config test-client test-server
ci-quick-test: test-raft test-raftpb test-rsm test-logdb test-transport \
  test-utils test-wrapper test-config test-client test-server
test: dragonboat-test test-drummer
slow-test: test-slow-multiraft test-slow-drummer
more-test: test test-slow-multiraft test-slow-drummer
monkey-test: test-monkey-multiraft test-monkey-drummer
dev-test: test test-grpc-transport test-cppwrapper

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
test-drummer:
	$(GOTEST) $(PKGNAME)/drummer
test-wrapper: $(PLUGIN_CPP_EXAMPLE_BIN)
	$(GOTEST) -o $(WRAPPER_TESTING_BIN) -c $(PKGNAME)/internal/cpp
	./$(WRAPPER_TESTING_BIN) -test.v
	rm -f ./$(WRAPPER_TESTING_BIN)

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
	$(GOTEST) -o $(DRUMMER_MONKEY_TESTING_BIN) -c $(PKGNAME)/drummer
	./$(DRUMMER_MONKEY_TESTING_BIN) -test.v -test.timeout 9999s

test-monkey-multiraft: TESTTAGVALS+=$(MULTIRAFT_MONKEY_TEST_BUILDTAGS)
test-monkey-multiraft:
	$(GOTEST) $(PKGNAME)

test-monkey-drummer: TESTTAGVALS+=$(DRUMMER_MONKEY_TEST_BUILDTAGS)
test-monkey-drummer: plugin-kvtest plugin-cppkvtest
	$(GOTEST) -o $(DRUMMER_MONKEY_TESTING_BIN) -c $(PKGNAME)/drummer
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
	$(GOTEST) $(BUILD_TEST_ONLY) $(PKGNAME)/drummer

monkey-drummer: TESTTAGVALS+=$(DRUMMER_MONKEY_TEST_BUILDTAGS)
monkey-drummer: plugin-kvtest
	$(GOTEST) $(BUILD_TEST_ONLY) $(PKGNAME)/drummer

all-slow-monkey-tests: slow-multiraft slow-drummer monkey-drummer

###############################################################################
# cpptest plugin
###############################################################################
CPPTEST_SRC=internal/tests/cpptest/example.cpp \
  internal/tests/cpptest/exampleplugin.cpp
CPPTEST_OBJS=$(subst .cpp,.o,$(CPPTEST_SRC))

internal/tests/cpptest/%.o: internal/tests/cpptest/%.cpp
	$(CXX) $(TEST_CXXFLAGS) -Iinternal/tests -Ibinding/include -c -o $@ $<

$(PLUGIN_CPP_EXAMPLE_BIN): $(CPPTEST_OBJS)
	$(CXX) $(TEST_LDFLAGS) $(CPPTEST_LDFLAGS) \
		-o $(PLUGIN_CPP_EXAMPLE_BIN) $(CPPTEST_OBJS)

###############################################################################
# cpptestkv 
###############################################################################
CPPKVTEST_AUTO_GEN_FILES=internal/tests/cppkv/kv.pb.cc \
  internal/tests/cppkv/kv.pb.h
CPPKVTEST_SRC=internal/tests/cppkv/kv.pb.cc \
	  internal/tests/cppkv/kvstore.cc internal/tests/cppkv/plugin.cc
CPPKVTEST_OBJS=$(subst .cc,.o,$(CPPKVTEST_SRC))

codegen-testkvpb:
	protoc -I internal/tests/kvpb \
		--cpp_out=internal/tests/cppkv internal/tests/kvpb/kv.proto

internal/tests/cppkv/kv.pb.cc: codegen-testkvpb

internal/tests/cppkv/%.o:internal/tests/cppkv/%.cc
	$(CXX) $(TEST_CXXFLAGS) -I/usr/local/include -Ibinding/include \
		-Iinternal/utils/cpp/include -c -o $@ $<

$(PLUGIN_CPP_KVTEST_BIN): $(CPPKVTEST_OBJS)
	$(CXX) $(CPPKVTEST_LDFLAGS) -o $(PLUGIN_CPP_KVTEST_BIN) $(CPPKVTEST_OBJS) \
		-L/usr/local/lib -lprotobuf 

# this target depends on the C/C++ version of protobuf, it is only used when
# testing the cppwrapper, users don't have to build this. 
plugin-cppkvtest: $(PLUGIN_CPP_KVTEST_BIN)

###############################################################################
# cppwrapper tests
###############################################################################
CPPWRAPPER_TEST_SRC = binding/tests/nodehost_tests.cpp \
  binding/tests/dragonboat_tests.cpp binding/tests/zupply.cpp
CPPWRAPPER_TEST_OBJS=$(subst .cpp,.o,$(CPPWRAPPER_TEST_SRC))
CPPWRAPPER_TEST_BIN=dragonboat-cppwrapper-tests
ASAN_OPTIONS ?= ASAN_OPTIONS=leak_check_at_exit=0:detect_container_overflow=0

binding/tests/%.o: binding/tests/%.cpp
	$(CXX) $(TEST_CXXFLAGS) -c -o $@ $<

$(CPPWRAPPER_TEST_BIN): binding $(PLUGIN_CPP_EXAMPLE_BIN) $(CPPWRAPPER_TEST_OBJS)
	$(CXX) $(TEST_LDFLAGS) -o $@ $(CPPWRAPPER_TEST_OBJS) \
		-lgtest -lgtest_main -L$(PKGROOT) -ldragonboatcpp \
		-L$(PKGROOT) -ldragonboat -lpthread -Wl,-rpath,.

# TODO:
# for libasan.so.2 and libasan.so.3, leak_check_at_exit used with go1.10/go1.11
# will cause Segfault
# the go part of the program doesn't have the std::vector instrumented, set
# detect_container_overflow=0 to avoid false positive
test-cppwrapper: $(CPPWRAPPER_TEST_BIN)
	$(ASAN_OPTIONS) ./$(CPPWRAPPER_TEST_BIN)
	
###############################################################################
# install/uninstall cpp binding
###############################################################################
BINDING_SRCS=binding/cpp/dragonboat.cpp binding/c/binding.c
BINDING_OBJS=binding/cpp/dragonboat.o binding/c/binding.o
BINDING_INC_PATH=binding/include/dragonboat
BINDING_STATIC_LIB=libdragonboatcpp.a
BINDING_BIN=$(BINDING_DIR)/$(SHARED1)
BINDING_BIN_HEADER=$(BINDING_DIR)/include/dragonboat/libdragonboat.h
BINDING_STATIC_LIB_CXXFLAGS=$(CXXFLAGS) $(BINDING_STATIC_LIB_TEST_CXXFLAGS)
BINDING_STATIC_LIB_CFLAGS=$(CFLAGS) $(BINDING_STATIC_LIB_TEST_CXXFLAGS)

binding/cpp/%.o: binding/cpp/%.cpp
	$(CXX) $(BINDING_STATIC_LIB_CXXFLAGS) -c -o $@ $<
binding/c/%.o: binding/c/%.c
	$(CC) $(BINDING_STATIC_LIB_CFLAGS) -c -o $@ $<
$(SHARED4):
	$(GO) build -tags=$(BINDINGTAGS) -o $(BINDING_BIN) -buildmode=c-shared \
		$(PKGNAME)/binding
	if [ $(OS) = "Darwin" ] ; \
  then \
    install_name_tool -id "@rpath/$(SHARED1)" $(BINDING_BIN); \
  fi
	ln -f -s $(PKGROOT)/binding/libdragonboat.h \
		$(BINDING_INC_PATH)/libdragonboat.h
	mv $(BINDING_BIN) $(SHARED4)
$(BINDING_OBJS): $(SHARED4)
$(BINDING_STATIC_LIB): $(SHARED4) $(BINDING_OBJS)
	ar rcs $(BINDING_STATIC_LIB) $(BINDING_OBJS)
$(SHARED1): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED1)
$(SHARED2): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED2)
$(SHARED3): $(SHARED4)
	ln -fs $(SHARED4) $(SHARED3)

binding: $(SHARED1) $(SHARED2) $(SHARED3) $(BINDING_STATIC_LIB)

FIND_HEADER_CMD=find "$(BINDING_INC_PATH)" \
	\( -type l -o -type f \) -name "*.h"
install-headers:
	install -d $(INSTALL_PATH)/lib
	install -d $(INSTALL_PATH)/include/dragonboat
	for header in `$(FIND_HEADER_CMD)`; \
	do \
		header=`basename $$header`; \
		install -C -m 644 $(BINDING_INC_PATH)/$$header \
			$(INSTALL_PATH)/include/dragonboat/$$header; \
	done
install-cpp-binding: $(BINDING_STATIC_LIB)
	install -C -m 755 $(BINDING_STATIC_LIB) $(INSTALL_PATH)/lib
install-binding: binding install-headers install-cpp-binding
	install -C -m 755 $(SHARED4) $(INSTALL_PATH)/lib && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED3) && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED2) && \
		ln -fs $(SHARED4) $(INSTALL_PATH)/lib/$(SHARED1)
uninstall-binding:
	rm -rf $(INSTALL_PATH)/include/dragonboat \
		$(INSTALL_PATH)/lib/$(BINDING_STATIC_LIB) \
		$(INSTALL_PATH)/lib/$(SHARED4) \
		$(INSTALL_PATH)/lib/$(SHARED3) \
		$(INSTALL_PATH)/lib/$(SHARED2) \
		$(INSTALL_PATH)/lib/$(SHARED1)

###############################################################################
# static checks
###############################################################################
CHECKED_PKGS=internal/raft internal/logdb internal/transport \
	internal/rsm internal/settings internal/tests internal/cpp \
	internal/tests/lcm internal/utils/lang internal/utils/random \
	internal/utils/fileutil internal/utils/syncutil \
	internal/utils/stringutil internal/utils/logutil internal/utils/netutil \
	internal/utils/cache internal/utils/envutil internal/utils/compression \
	internal/server drummer/server/drummer drummer/server/nodehost \
	drummer/server/drummercmd raftpb \
	logger raftio config binding statemachine drummer client drummer/client
CPP_CHECKED_DIRS=$(BINDING_INC_PATH)/*.h binding/cpp/*.cpp \
	internal/cpp/*.h internal/cpp/*.cpp
CPPLINT_FILTERS=--filter=-whitespace/braces,-build/include,-build/c++11

static-check: cpp-static-check
	$(GO) vet -tests=false $(PKGNAME)
	golint $(PKGNAME)
	@for p in $(CHECKED_PKGS); do \
		if [ $$p = "internal/logdb" ] ; \
		then \
			$(GO) vet -tags="$(LOGDB_TEST_BUILDTAGS)" $(PKGNAME)/$$p; \
		elif [ $$p = "binding" ] ; \
		then \
			$(GO) vet -tags="$(BINDING_TAG)" $(PKGNAME)/$$p; \
		else \
			$(GO) vet $(PKGNAME)/$$p; \
		fi; \
		golint $$p; \
		ineffassign $$p; \
		if [ $$p != "drummer" ] ; \
		then \
			gocyclo -over 41 $$p; \
		fi; \
		gosec -quiet -exclude G103,G204,G401,G404,G501 $$p; \
	done;

cpp-static-check:
	@for d in $(CPP_CHECKED_DIRS); do \
		for f in `ls $$d`; do \
			if [ $$f != "$(BINDING_INC_PATH)/libdragonboat.h" ] ; \
			then \
				python scripts/cpplint.py --quiet $(CPPLINT_FILTERS) $$f; \
			fi; \
		done; \
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
		$(DRUMMER_SERVER_BIN) \
		$(NODEHOST_SERVER_BIN) \
		$(DRUMMER_CMD_BIN) \
		$(PLUGIN_KVSTORE_BIN) \
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
	$(DRUMMER_SERVER_BIN) $(NODEHOST_SERVER_BIN) $(DRUMMER_CMD_BIN) \
	$(PLUGIN_CPP_KVTEST_BIN) $(DRUMMER_MONKEY_TESTING_BIN) \
	$(MULTIRAFT_MONKEY_TESTING_BIN) $(PLUGIN_KVSTORE_BIN) \
	$(PORCUPINE_CHECKER_BIN) $(LOGDB_CHECKER_BIN) \
	plugin-cppkvtest drummer-monkey-test-bin binding test servers \
	test-raft test-rsm test-logdb test-transport test-multiraft test-drummer \
	test-session test-server test-utils test-config test-cppwrapper \
	static-check cpp-static-check clean plugin-kvtest logdb-checker \
	test-monkey-drummer test-wrapper test-slow-multiraft test-grpc-transport \
	test-slow-drummer slow-test more-test monkey-test dev-test \
	slow-multiraft-ioerror-test-bin all-slow-monkey-tests \
	gen-test-docker-images docker-test dragonboat-test snapshot-benchmark-test \
	docker-test-ubuntu-stable docker-test-go-old docker-test-debian-testing \
	docker-test-debian-stable docker-test-centos-stable docker-test-min-deps
