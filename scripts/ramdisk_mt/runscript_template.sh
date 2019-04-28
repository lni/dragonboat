#!/bin/bash

# Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

OS=`uname`
TESTNAME="TestOnDiskClusterCanSurviveDrummerMonkeyPlay"
curdir=`pwd`
[[ $curdir =~ test([0-9]+)$ ]] && testno=${BASH_REMATCH[1]}

if [ $OS = "Linux" ]; then
  numaregex="([0-9]+)$"
  numarec=`lscpu | grep "NUMA node(s)"`
  count="1"
  if [[ $numarec =~ $numaregex ]] ; then
    count=${BASH_REMATCH[1]}
  fi
  numacmd=""
  if [[ $count == "2" ]] ; then
	  taskid=$(($i + 0))
 	  if [ $((testno%2)) -eq 0 ] ; then
      numacmd="numactl --cpunodebind=0 --localalloc"
 	  else
      numacmd="numactl --cpunodebind=1 --localalloc"
    fi
  fi
else
  numacmd=""
fi

for i in `seq 1 100`;
do
  fn="drummer-mt-error-$i.txt"
  echo "numa settings $numacmd" > numa.txt
  echo "iteration $i" > progress.txt
  settings="-test.timeout 2450s -test.v -test.run $TESTNAME"
  DRUMMERMTPORT=BASEPORT EXTERNALFILETEST=1 $numacmd ./drummer-monkey-testing $settings > drummer-mt-out.txt 2>&1
  if [ $? -ne 0 ]; then
    mv drummer-mt-out.txt $fn
    mv drummer_mt_pwd_safe_to_delete drummer_mt_pwd_safe_to_delete_err_$i
    cp external-*.data drummer_mt_pwd_safe_to_delete_err_$i/
  fi
  if [ -f drummer-lcm.jepsen ]; then
    ./porcupine-checker-bin -path drummer-lcm.jepsen -timeout 30
    if [ $? -ne 0 ]; then
      echo "" > linearizability-checker-error-$i.txt
      mv drummer-lcm.jepsen drummer-lcm-error-$i.jepsen
    fi
  fi
  ednfn="../lcmlog/drummer-lcm-$i-$testno.edn"
  jepsenfn="../lcmlog/drummer-lcm-$i-$testno.jepsen" 
  cp drummer-lcm.edn $ednfn
  cp drummer-lcm.jepsen $jepsenfn
  rm -f external-*.data
  rm -rf test_rocksdb_db_safe_to_delete
done

