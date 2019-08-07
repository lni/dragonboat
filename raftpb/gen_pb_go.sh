#!/bin/bash

# we are assuming that gogoprotobuf src is available next to dragonboat's code
# e.g. if dragonboat code is available at ~/src/dragonboat, gogoprotobuf is
# assumed to be available at ~/src/github.com/gogo/protobuf
/usr/local/bin/protoc --proto_path=..:../..:. --gogofaster_out=plugins=grpc:. raft.proto
