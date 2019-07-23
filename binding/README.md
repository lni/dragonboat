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

[JasonYuchen](https://github.com/JasonYuchen) provided some excellent [examples](https://github.com/JasonYuchen/dragonboat-cpp-example) on how to use the C++11 binding.

## Status ##
The C++ binding is in BETA status. The limitations are:

* Custom Log DB or Raft RPC modules are not supported.
* Custom logger is not supported.

## Contributors ##

[JasonYuchen](https://github.com/JasonYuchen) contributed the dragonboat v3.x support for the C++11 binding.
