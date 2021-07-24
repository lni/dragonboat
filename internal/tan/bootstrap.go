// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"bytes"
	"io"

	"github.com/cockroachdb/errors/oserror"

	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/vfs"
)

func getBootstrap(fs vfs.FS, dirname string,
	clusterID uint64, nodeID uint64) (rec pb.Bootstrap, err error) {
	filename := makeBootstrapFilename(fs, dirname, clusterID, nodeID, false)
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
	clusterID uint64, nodeID uint64, rec pb.Bootstrap) (err error) {
	buf := pb.MustMarshal(&rec)
	fn := makeBootstrapFilename(fs, dirname, clusterID, nodeID, true)
	ffn := makeBootstrapFilename(fs, dirname, clusterID, nodeID, false)
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
	clusterID uint64, nodeID uint64) error {
	fn := makeBootstrapFilename(fs, dirname, clusterID, nodeID, false)
	if _, err := fs.Stat(fn); oserror.IsNotExist(err) {
		return nil
	}
	if err := fs.RemoveAll(fn); err != nil {
		return err
	}
	return dataDir.Sync()
}
