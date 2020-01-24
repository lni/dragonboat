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
ifeq ($(DRAGONBOAT_LOGDB),leveldb)
$(info using leveldb based log storage)
GOCMD=$(GOEXEC)
LOGDB_TAG=dragonboat_leveldb_test
else ifeq ($(DRAGONBOAT_LOGDB),pebble)
GOCMD=$(GOEXEC)
LOGDB_TAG=dragonboat_pebble_test
else ifeq ($(DRAGONBOAT_LOGDB),pebble_memfs)
$(error invalid DRAGONBOAT_LOGDB pebble_memfs)
else ifeq ($(DRAGONBOAT_LOGDB),custom)
$(info using custom lodb)
GOCMD=$(GOEXEC)
LOGDB_TAG=dragonboat_no_rocksdb
else ifeq ($(DRAGONBOAT_LOGDB),)
GOCMD=$(GOEXEC)
ifneq ($(DRAGONBOAT_MEMFS_TEST),)
$(info using memfs based pebble)
LOGDB_TAG=dragonboat_memfs_test
else
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
endif
endif

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
# RACE=1 make test
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

INSTALL_PATH ?= /usr/local
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
DUMMY_TEST_BIN=test.bin

# go build tags
GOBUILDTAGVALS+=$(LOGDB_TAG)
GOBUILDTAGS="$(GOBUILDTAGVALS)"
TESTTAGVALS+=$(GOBUILDTAGVALS)
TESTTAGVALS+=$(LOGDB_TEST_BUILDTAGS)
TESTTAGS="$(TESTTAGVALS)"
$(info build tags are set to $(GOBUILDTAGS))
LOGDB_TEST_BUILDTAGS=dragonboat_logdbtesthelper

all:
rebuild-all: clean unit-test-bin

cross-rebuild: LOGDB_TAG=dragonboat_no_rocksdb
cross-rebuild: cross-build-bin
cross-rebuild-win: EXTNAME=win
cross-rebuild-win: GO=GOOS=windows $(GOCMD)
cross-rebuild-win: cross-rebuild
cross-rebuild-darwin: EXTNAME=darwin
cross-rebuild-darwin: GO=GOOS=darwin $(GOCMD)
cross-rebuild-darwin: cross-rebuild
cross-rebuild-freebsd: EXTNAME=freebsd
cross-rebuild-freebsd: GO=GOOS=freebsd $(GOCMD)
cross-rebuild-freebsd: cross-rebuild
###############################################################################
# download and install rocksdb
###############################################################################
LIBCONF_PATH=/etc/ld.so.conf.d/usr_local_lib.conf
# set the variables below to tell the Makefile which version of rocksdb you
# want to install. rocksdb v5.13.4 is the version we used in production, it is
# used here as the default, feel free to change to the version number you like
#
# rocksdb 6.3.x or 6.4.x is required
ROCKSDB_MAJOR_VER=6
ROCKSDB_MINOR_VER=4
ROCKSDB_PATCH_VER=6
ROCKSDB_VER ?= $(ROCKSDB_MAJOR_VER).$(ROCKSDB_MINOR_VER).$(ROCKSDB_PATCH_VER)

RDBTMPDIR=$(PKGROOT)/build/rocksdbtmp
RDBURL=https://github.com/facebook/rocksdb/archive/v$(ROCKSDB_VER).tar.gz
build-rocksdb: get-rocksdb make-rocksdb
build-rocksdb-static: get-rocksdb make-rocksdb-static
build-original-rocksdb : get-rocksdb make-rocksdb
get-rocksdb:
	@{ \
		set -e; \
		rm -rf $(RDBTMPDIR); \
		mkdir -p $(RDBTMPDIR); \
		wget $(RDBURL) -P $(RDBTMPDIR); \
		tar xzvf $(RDBTMPDIR)/v$(ROCKSDB_VER).tar.gz -C $(RDBTMPDIR); \
	}
make-rocksdb:
	@EXTRA_CXXFLAGS=-DROCKSDB_NO_DYNAMIC_EXTENSION make -C $(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) -j8 shared_lib
make-rocksdb-static:
	@EXTRA_CXXFLAGS=-DROCKSDB_NO_DYNAMIC_EXTENSION make -C $(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) -j8 static_lib
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
install-rocksdb-lib-ull-static:
	@{ \
    set -e; \
    sudo INSTALL_PATH=/usr/local make -C \
      $(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) install-static; \
    rm -rf $(RDBTMPDIR); \
  }
do-install-rocksdb-ull: install-rocksdb-lib-ull ldconfig-rocksdb-lib-ull
do-install-rocksdb:
	@(INSTALL_PATH=$(PKGROOT)/build make -C \
		$(RDBTMPDIR)/rocksdb-$(ROCKSDB_VER) install-shared && rm -rf $(RDBTMPDIR))

install-rocksdb-ull-darwin: build-rocksdb install-rocksdb-ull
install-rocksdb-ull: build-rocksdb do-install-rocksdb-ull
install-rocksdb-ull-static: build-rocksdb-static install-rocksdb-lib-ull-static
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
	docker build -t dragonboat-ubuntu-no-rocksdb:18.04 -f scripts/Dockerfile-no-rocksdb .

DOCKERROOTDIR="/go/src/github.com/lni/dragonboat"
DOCKERRUN=docker run --rm -v $(PKGROOT):$(DOCKERROOTDIR)
docker-test: docker-test-ubuntu-stable docker-test-debian-testing \
	docker-test-debian-stable docker-test-centos-stable docker-test-go-old \
	docker-test-min-deps docker-test-no-rocksdb
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
docker-test-no-rocksdb: clean
	$(DOCKERRUN) -t dragonboat-ubuntu-no-rocksdb:18.04

###############################################################################
# tests
###############################################################################
TEST_OPTIONS=test -tags=$(TESTTAGS) -count=1 $(VERBOSE) \
	$(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION)
BUILD_TEST_ONLY=-c -o test.bin 
dragonboat-test: test-raft test-raftpb test-rsm test-logdb test-transport \
	test-multiraft test-config test-client test-server test-tools
travis-ci-test: test-raft test-raftpb test-rsm test-logdb test-transport \
  test-config test-client test-server test-tests test-tools
test: dragonboat-test test-plugins test-tests
dev-test: test

###############################################################################
# build unit tests
###############################################################################
unit-test-bin: TEST_OPTIONS=test -c -o $@.bin -tags=$(TESTTAGS) 						 \
	-count=1 $(VERBOSE) $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION) 
unit-test-bin: test-raft test-raftpb test-rsm test-logdb test-transport 		 \
  test-multiraft test-config test-client test-server test-tools test-plugins \
	test-tests

cross-build-bin: TEST_OPTIONS=test -c -o $@.$(EXTNAME) -tags=$(TESTTAGS)       \
  -count=1 $(VERBOSE) $(RACE_DETECTOR_FLAG) $(SELECTED_TEST_OPTION)
cross-build-bin: test-raft test-raftpb test-rsm test-logdb test-transport      \
  test-multiraft test-config test-client test-server test-tools test-tests

###############################################################################
# fast tests executed for every git push
###############################################################################
benchmark:
	$(GOTEST) $(SELECTED_BENCH_OPTION)

benchmark-fsync:
	$(GOTEST)	-run ^$$ -bench=BenchmarkFSyncLatency

GOTEST=$(GO) $(TEST_OPTIONS)
test-plugins:
	$(GOTEST) $(PKGNAME)/plugin
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
test-rsm:
	$(GOTEST) $(PKGNAME)/internal/rsm
test-logdb:
	$(GOTEST) $(PKGNAME)/internal/logdb
test-transport:
	$(GOTEST) $(PKGNAME)/internal/transport
test-multiraft:
	$(GOTEST) $(PKGNAME)
test-tests:
	$(GOTEST) $(PKGNAME)/internal/tests
test-tools:
	$(GOTEST) $(PKGNAME)/tools

###############################################################################
# snapshot benchmark test to check actual bandwidth achieved when streaming
# snapshot images
###############################################################################
snapshot-benchmark-test:
	$(GO) build -v -o $(SNAPSHOT_BENCHMARK_TESTING_BIN) \
		$(PKGNAME)/internal/tests/snapshotbench

###############################################################################
# language bindings
###############################################################################
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
else ifeq ($(OS),FreeBSD)
SANITIZER_FLAGS ?= -fsanitize=address -fsanitize=leak
else
$(error OS type $(OS) not supported)
endif
BINDING_STATIC_LIB_TEST_CXXFLAGS=-g $(SANITIZER_FLAGS)
endif

CXXSTD=-std=c++11
CFLAGS=-Wall -fPIC -O3 -Ibinding/include
CXXFLAGS=$(CXXSTD) -Wall -fPIC -O3 -Ibinding/include
TEST_CXXFLAGS=$(CXXFLAGS) -g $(SANITIZER_FLAGS)
TEST_LDFLAGS=$(SANITIZER_FLAGS)
LIBNAME=libdragonboat
PLATFORM_SHARED_EXT=so
BINDING_DIR=binding
BINDING_TAG=dragonboat_language_binding
BINDINGTAGS="$(GOBUILDTAGVALS) $(BINDING_TAG)"
SHARED_MAJOR=$(shell egrep "DragonboatMajor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_MINOR=$(shell egrep "DragonboatMinor = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED_PATCH=$(shell egrep "DragonboatPatch = [0-9]" nodehost.go | cut -d ' ' -f 3)
SHARED1=${LIBNAME}.$(PLATFORM_SHARED_EXT)
SHARED2=$(SHARED1).$(SHARED_MAJOR)
SHARED3=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR)
SHARED4=$(SHARED1).$(SHARED_MAJOR).$(SHARED_MINOR).$(SHARED_PATCH)
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

install-headers:
	install -d $(INSTALL_PATH)/lib
	for header_dir in `cd $(BINDING_INC_PATH)/..; \
		find "dragonboat" -type l -o -type d`; do \
			install -d $(INSTALL_PATH)/include/$$header_dir; \
	done
	for header in `cd $(BINDING_INC_PATH)/..; \
		find "dragonboat" -type l -o -type f -name "*.h"`; do \
			install -C -m 644 $(BINDING_INC_PATH)/../$$header \
				$(INSTALL_PATH)/include/$$header; \
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

# tests for the C++11 binding
PLUGIN_CPP_EXAMPLE_BIN=dragonboat-cpp-plugin-example.so
ifeq ($(OS),Darwin)
CPPTEST_LDFLAGS=-bundle -undefined dynamic_lookup \
  -Wl,-install_name,$(PLUGIN_CPP_EXAMPLE_BIN)
else ifeq ($(OS),Linux)
CPPTEST_LDFLAGS=-shared -Wl,-soname,$(PLUGIN_CPP_EXAMPLE_BIN)
else ifeq ($(OS),FreeBSD)
CPPTEST_LDFLAGS=-shared -Wl,-soname,$(PLUGIN_CPP_EXAMPLE_BIN)
else ifneq (,$(findstring MINGW,$(OS)))
else ifneq (,$(findstring MSYS, $(OS)))
else
$(error OS type $(OS) not supported)
endif

TEST_LDFLAGS=$(SANITIZER_FLAGS)
CPPTEST_SRC=internal/tests/cpptest/example.cpp \
  internal/tests/cpptest/exampleplugin.cpp
CPPTEST_OBJS=$(subst .cpp,.o,$(CPPTEST_SRC))
CPPWRAPPER_TEST_SRC=binding/tests/nodehost_tests.cpp \
  binding/tests/dragonboat_tests.cpp binding/tests/zupply.cpp
CPPWRAPPER_TEST_OBJS=$(subst .cpp,.o,$(CPPWRAPPER_TEST_SRC))
CPPWRAPPER_TEST_BIN=dragonboat-cppwrapper-tests
WRAPPER_TESTING_BIN=cpp-wrapper-test.bin
ASAN_OPTIONS ?= ASAN_OPTIONS=leak_check_at_exit=0:detect_container_overflow=0

internal/tests/cpptest/%.o: internal/tests/cpptest/%.cpp
	$(CXX) $(TEST_CXXFLAGS) -Iinternal/tests -Ibinding/include -c -o $@ $<
$(PLUGIN_CPP_EXAMPLE_BIN): $(CPPTEST_OBJS)
	$(CXX) $(TEST_LDFLAGS) $(CPPTEST_LDFLAGS) \
		-o $(PLUGIN_CPP_EXAMPLE_BIN) $(CPPTEST_OBJS)
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

test-wrapper: $(PLUGIN_CPP_EXAMPLE_BIN)
	$(GOTEST) -o $(WRAPPER_TESTING_BIN) -c $(PKGNAME)/internal/cpp
	./$(WRAPPER_TESTING_BIN) -test.v

clean-binding:
	@rm -f $(SHARED1) $(SHARED2) $(SHARED3) $(SHARED4) \
		$(BINDING_BIN) $(BINDING_OBJS) $(BINDING_STATIC_LIB) \
		$(CPPWRAPPER_TEST_BIN) $(PLUGIN_CPP_EXAMPLE_BIN) $(CPPWRAPPER_TEST_OBJS) \
		$(CPPTEST_OBJS) $(WRAPPER_TESTING_BIN)

###############################################################################
# static checks
###############################################################################
CHECKED_PKGS=internal/raft internal/logdb internal/logdb/kv internal/transport \
	internal/cpp internal/rsm internal/settings internal/tests internal/server   \
	internal/logdb/kv/rocksdb internal/logdb/kv/pebble internal/logdb/kv/leveldb \
	plugin/pebble plugin/rocksdb raftpb tools binding logger raftio config       \
	statemachine client plugin/leveldb internal/utils/dio internal/vfs

static-check:
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

GOLANGCI_LINT_PKGS=internal/raft internal/rsm internal/cpp internal/transport  \
	internal/server statemachine tools raftpb raftio client tools logger config  \
	internal/logdb/kv/rocksdb internal/logdb/kv/pebble internal/logdb/kv/leveldb \
	plugin/rocksdb plugin/leveldb plugin/pebble internal/settings internal/tests \
	internal/logdb/kv internal/utils/dio internal/vfs

golangci-lint-check:
	@for p in $(GOLANGCI_LINT_PKGS); do \
		golangci-lint run $$p; \
	done;
	@golangci-lint run .
	@golangci-lint run --build-tags=dragonboat_language_binding binding
	@golangci-lint run --build-tags=dragonboat_logdbtesthelper internal/logdb

###############################################################################
# clean
###############################################################################
clean: clean-binding
	@find . -type d -name "*safe_to_delete" -print | xargs rm -rf
	@rm -f gitversion.go 
	@rm -f test-*.bin test-*.freebsd test-*.win test-*.darwin
	@rm -f $(SEQUENCE_TESTING_BIN) \
		$(DUMMY_TEST_BIN) \
		$(DUMMY_TEST_BIN) \
		$(SNAPSHOT_BENCHMARK_TESTING_BIN) \
		$(MULTIRAFT_ERROR_INJECTION_TESTING_BIN) \
		$(PORCUPINE_CHECKER_BIN) $(LOGDB_CHECKER_BIN)

.PHONY: gen-gitversion install-dragonboat install-rocksdb \
	test test-raft test-rsm test-logdb test-tools test-transport test-multiraft \
	test-client test-server test-config test-tests static-check clean \
	logdb-checker golangci-lint-check \
	gen-test-docker-images docker-test dragonboat-test snapshot-benchmark-test \
	docker-test-ubuntu-stable docker-test-go-old docker-test-debian-testing \
	docker-test-debian-stable docker-test-centos-stable docker-test-min-deps \
	docker-test-no-rocksdb travis-ci-test dev-test \
	cross-rebuild cross-rebuild-win cross-rebuild-darwin cross-rebuild-freebsd
