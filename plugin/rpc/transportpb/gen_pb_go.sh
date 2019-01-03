#!/bin/bash

/usr/local/bin/protoc --go_out=plugins=grpc:. --proto_path=../..:$GOPATH/src:. transport.proto
