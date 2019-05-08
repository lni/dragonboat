## About ##
This is the package used for supporting foreign language bindings.

C++11 is currently supported. Python support is being worked on.

## Getting Started ##
To install the C++ binding:
```
$ cd $GOPATH/src/github.com/lni/dragonboat
$ make binding
$ sudo make install-binding
```
This will install the C++ binding to /usr/local/include and /usr/local/lib by default, use the INSTALL_PATH variable to specify where to install the header and lib files.  
To uninstall the C++ binding from its default location:
```
sudo make uninstall-binding
```
Run C++ binding tests (gtest is required):
```
$ cd $GOPATH/src/github.com/lni/dragonboat
$ make clean
$ make test-cppwrapper
```
We also have an [example program](https://github.com/lni/dragonboat-example) showing how to use the C++11 binding.

## Status ##
The C++ binding is in BETA status.

There are some minor limitations:
* Custom Log DB or Raft RPC modules are not supported.
* Custom logger is not supported.
* IConcurrentStateMachine and IOnDiskStateMachine are not supported.
