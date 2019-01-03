#!/bin/bash

# check whether charybdefs is running as expected
ps aux | grep "[c]harybdefs" >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "charybdefs is not running" >&2; exit 1
fi

if [ -z $EITHOME ]; then
  echo "EITHOME not set" >&2; exit 1
fi
if [ -z $EITDATAHOME ]; then
  echo "EITDATAHOME not set" >&2; exit 1
fi

ts=`date`
echo $ts > $EITDATAHOME/mirrortest.tmp
if [ $? -ne 0 ]; then
  echo "failed to write the mirror test file" >&2; exit 1
fi
ts=`cat $EITDATAHOME/mirrortest.tmp`
mirrored=`cat $EITHOME/mirrortest.tmp`
rm $EITDATAHOME/mirrortest.tmp
if [ "$ts" != "$mirrored" ]; then
  echo "FUSE is not properly configured" >&2; exit 1
fi

# build and run tests
make slow-multiraft-ioerror-test-bin
if [ $? -ne 0 ]; then
  echo "failed to build the test executable" >&2; exit 1
fi

snapshotReadTest='TestFailedSnapshotReadIsReported'
snapshotReadRE='panic: failed to restore from snapshot'
snapshotRenameTest='TestFailedSnapshotTempFileRenameIsReported'
snapshotRenameRE='panic: rename.+[0-9A-Z]+: no space left on device'
snapshotChunkTest='TestFailedSnapshotChunkSaveIsReported'
snapshotChunkRE='panic:.+[0-9A-Z]+\.gbsnap: no space left on device'
nodehostIdReadTest='TestFailedNodeHostIDFileReadIsReported'
nodehostIdReadRE='panic:.+dragonboat\.address: input/output error'
nodehostIdTest='TestFailedNodeHostIDFileWriteIsReported'
nodehostIdRE='panic:.+\.address: no space left on device'
snapshotWriteTest='TestFailedSnapshotWritesAreReported'
snapshotWriteRE='panic:.+[0-9A-Z]+\.gbsnap: no space left on device'
rdbENOSPCTest='TestFailedRDBWritesAreReportedENOSPC'
rdbENOSPCRE='panic: IO error:.+[0-9A-Z]+\.log: No space left on device'
rdbReadTest='TestFailedRDBReadIsReported'
rdbReadRE='panic: IO error:.+[0-9A-Z]\.sst: Input/output error'

tests=("$snapshotWriteTest" \
  "$nodehostIdTest" \
  "$nodehostIdReadTest" \
  "$rdbENOSPCTest" \
  "$rdbReadTest" \
  "$snapshotChunkTest" \
  "$snapshotRenameTest" \
  "$snapshotReadTest")
res=("$snapshotWriteRE" \
  "$nodehostIdRE" \
  "$nodehostIdReadRE" \
  "$rdbENOSPCRE" \
  "$rdbReadRE" \
  "$snapshotChunkRE" \
  "$snapshotRenameRE" \
  "$snapshotReadRE")

# check output
for i in $(seq 0 $((${#tests[*]}-1)))
do
  testname=${tests[i]}
  revalue=${res[i]}
  echo "RUN: $testname"
  IOEI=1 ./multiraft-ioei-testing -test.run $testname > ioetest.tmp 2>&1
  content=`cat ioetest.tmp`
  rm ioetest.tmp
  if [[ $content =~ $revalue ]]; then
    echo "PASS"
  else
    echo "FAILED $content" >&2; exit 1
  fi
done
rm multiraft-ioei-testing
