#!/bin/bash

/usr/local/bin/protoc --proto_path=../..:../vendor:$GOPATH/src:. --gogofaster_out=plugins=grpc:. drummer.proto
