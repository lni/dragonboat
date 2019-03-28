#!/bin/bash

/usr/local/bin/protoc --proto_path=..:$GOPATH/src:. --gogofaster_out=. kv.proto
