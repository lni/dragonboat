# Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import multiprocessing
import commands
import signal
import re
import os
import os.path
import argparse

def getKnossosFiles(dirname):
    fnre = re.compile(r'.edn$')
    return [os.path.join(dirname, f) for f in os.listdir(dirname) if fnre.search(f)]

def knossosCheckWorker(fn):
    cmd = 'lein run --model cas-register %s' % fn
    p = commands.getstatusoutput(cmd)
    return (p[0], p[1], fn)

def signalHandler(signal, frame):
    print("CTRL+C pressed")
    raise RuntimeError("stopped by user")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--worker", type=int, default=12, help="# of workers to use")
    parser.add_argument("-k", "--knossos", type=str, help="knossos installation folder")
    parser.add_argument("-d", "--data", type=str, help="edn data folder")
    args = parser.parse_args()
    
    edndir = args.data
    if edndir is None or len(edndir) == 0:
        raise RuntimeError("edn folder not specified")
    knossosdir = args.knossos
    if knossosdir is None or len(knossosdir) == 0:
        raise RuntimeError("knossos installation dir not specified")
    worker = args.worker
    if worker <= 0:
        raise RuntimeError("invalid worker count")

    files = getKnossosFiles(edndir)
    if len(files) == 0:
        raise RuntimeError("no .edn file found in %s" % edndir)
    
    total = len(files)    
    signal.signal(signal.SIGINT, signalHandler)
    os.chdir(knossosdir)
    p = multiprocessing.Pool(worker)
    completed = 0
    for i in p.imap_unordered(knossosCheckWorker, files):
        completed = completed +1
        if 'false' in i[1]:
            raise RuntimeError("non-linearizable failure found in %s" % i[2])
        if i[0] == 0:
            result = "done"
        else:
            result = i[1]
        print("edn %40s, result %s, %d/%d" % (i[2], result, completed, total))
    
if __name__ == '__main__':
    main()
