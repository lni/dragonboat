#!/bin/bash

/usr/local/bin/protoc --go_out=. --proto_path=../..:$GOPATH/src:. kv.proto
