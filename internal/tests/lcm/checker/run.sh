#!/bin/bash

set -e
for f in `ls /media/drummermt-ramdisk-test/lcmlog/*.jepsen`
do
  echo "checking $f"
  ./checker -path $f -timeout 30
done
