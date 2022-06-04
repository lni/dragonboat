// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tan

import (
	"bytes"
	"io"

	"github.com/cockroachdb/errors/oserror"

	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
)

// getBootstrap returns saved bootstrap record. Bootstrap records are saved
// side by side with the tan db itself, so strictly speaking, they are not
// a part of the tan db.
func getBootstrap(fs vfs.FS, dirname string,
	shardID uint64, replicaID uint64) (rec pb.Bootstrap, err error) {
	filename := makeBootstrapFilename(fs, dirname, shardID, replicaID, false)
	f, err := fs.Open(filename)
	if err != nil {
		if oserror.IsNotExist(err) {
			return pb.Bootstrap{}, raftio.ErrNoBootstrapInfo
		}
		return pb.Bootstrap{}, err
	}
	defer func() {
		err = firstError(err, f.Close())
	}()
	r := newReader(f, fileNum(0))
	rr, err := r.next()
	if err != nil {
		return pb.Bootstrap{}, err
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rr); err != nil {
		return pb.Bootstrap{}, err
	}
	pb.MustUnmarshal(&rec, buf.Bytes())
	return rec, nil
}

func saveBootstrap(fs vfs.FS,
	dirname string, dataDir vfs.File,
	shardID uint64, replicaID uint64, rec pb.Bootstrap) (err error) {
	buf := pb.MustMarshal(&rec)
	fn := makeBootstrapFilename(fs, dirname, shardID, replicaID, true)
	ffn := makeBootstrapFilename(fs, dirname, shardID, replicaID, false)
	f, err := fs.Create(fn)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.Sync())
		err = firstError(err, f.Close())
		if err == nil {
			err = fs.Rename(fn, ffn)
		}
		err = firstError(err, dataDir.Sync())
	}()
	w := newWriter(f)
	defer func() {
		err = firstError(err, w.close())
	}()
	if _, err = w.writeRecord(buf); err != nil {
		return err
	}
	return nil
}

func removeBootstrap(fs vfs.FS, dirname string, dataDir vfs.File,
	shardID uint64, replicaID uint64) error {
	fn := makeBootstrapFilename(fs, dirname, shardID, replicaID, false)
	if _, err := fs.Stat(fn); oserror.IsNotExist(err) {
		return nil
	}
	if err := fs.RemoveAll(fn); err != nil {
		return err
	}
	return dataDir.Sync()
}
