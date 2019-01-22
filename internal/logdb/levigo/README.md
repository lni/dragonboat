
# LevelDB

This repo is a fork from [DataDog/leveldb](https://github.com/DataDog/leveldb),
while [DataDog/leveldb](https://github.com/DataDog/leveldb) itself is a fork
of [jmhodges/levigo](https://github.com/jmhodges/levigo). 

This fork makes levigo go-gettable by including the C++ source for LevelDB
and building it with the Go code.  LevelDB and Snappy are both compiled
and linked statically, so while you will not need them installed on your
target machine, you _should_ have a roughly compatible version of `libstdc++`.

This package currently includes LevelDB 1.19, and supports Linux and OS X.
It should work on Go 1.5 or greater.

[godoc documentation](http://godoc.org/github.com/DataDog/leveldb).

## Building

This package supports Linux and OS X.

You'll need gcc and g++. Then:

`go get github.com/DataDog/leveldb`

This will take a long time as it compiles the LevelDB source code along with the
Go library.

To avoid waiting for compilation every time you want to build your project, you can run:

`go install github.com/DataDog/leveldb`


## Updating Vendored Snappy

1. Overwrite source in `./deps/snappy/`
1. `make link_snappy`

If nothing significant in snappy has changed, things should JustWork&trade;

## Updating Vendored LevelDB

The embedded LevelDB source code is located in `deps/leveldb`. In order to get
`go build` to compile LevelDB, we symlink the required LevelDB source files to
the root directory of the project. These symlinks are prefixed with `deps_`.

To change the embedded version of LevelDB, do the following:

1. `rm deps_*.cc`
1. Replace `deps/leveldb` with the source code of the desired version of LevelDB
1. On Linux, run `./deps/leveldb/build_detect_platform linux.mk build_flags`
1. On OS X, run `./deps/leveldb/build_detect_platform darwin.mk build_flags`
1. `cat build_flags/*`, take a quick look at the new compiler and linker flags
   and see if there are any flags we're missing in `cgo_flags_*.go`. If so, add
   them.
1. `make link_leveldb`
1. On OS X and Linux, run `go build` and verify that everything compiles.

_note_ I couldn't get `build_detect_platform` to run as advertised above -- j

## Caveats

Comparators and WriteBatch iterators must be written in C in your own
library. This seems like a pain in the ass, but remember that you'll have the
LevelDB C API available to your in your client package when you import levigo.

An example of writing your own Comparator can be found in
<https://github.com/DataDog/leveldb/blob/master/examples>.
