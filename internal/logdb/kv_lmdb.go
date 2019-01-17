// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

package logdb

import (
	"bytes"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"

	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/raftio"
)

type lmdbKV struct {
	env *lmdb.Env
	dbi lmdb.DBI
}

func openLMDB(dir string, wal string) (*lmdbKV, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		return nil, err
	}
	err = env.SetMapSize(1 << 30)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = fileutil.MkdirAll(dir); err != nil {
			return nil, err
		}
	}
	err = env.Open(dir, 0, 0644)
	if err != nil {
		return nil, err
	}
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		return nil, err
	}
	if staleReaders > 0 {
		plog.Infof("cleared %d reader slots from dead processes", staleReaders)
	}
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("logdb")
		return err
	})
	return &lmdbKV{env: env, dbi: dbi}, nil
}

func (l *lmdbKV) Close() error {
	return l.env.Close()
}

func (l *lmdbKV) SaveValue(key []byte, val []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(l.dbi, key, val, 0)
	})
}

func (l *lmdbKV) GetValue(key []byte, op func([]byte) error) error {
	return l.env.View(func(txn *lmdb.Txn) error {
		v, err := txn.Get(l.dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return op(nil)
			}
			return err
		}
		return op(v)
	})
}

func (l *lmdbKV) DeleteValue(key []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Del(l.dbi, key, nil)
	})
}

func (l *lmdbKV) GetWriteBatch(ctx raftio.IContext) IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return wb.(*simpleWriteBatch)
		}
	}
	return newSimpleWriteBatch()
}

func (l *lmdbKV) CommitWriteBatch(wb IWriteBatch) error {
	swb, ok := wb.(*simpleWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return l.env.Update(func(txn *lmdb.Txn) error {
		for _, pair := range swb.vals {
			err := txn.Put(l.dbi, pair.key, pair.val, 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (l *lmdbKV) RemoveEntries(fk []byte, lk []byte) error {
	return nil
}

func (l *lmdbKV) Compaction(fk []byte, lk []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(l.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		_, _, err = cur.Get(fk, nil, lmdb.SetRange)
		if err != nil {
			return err
		}
		for {
			k, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if bytes.Compare(k, lk) != -1 {
				return nil
			}
			err = cur.Del(0)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (l *lmdbKV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) {
	err := l.env.View(func(txn *lmdb.Txn) error {
		scanner := lmdbscan.New(txn, l.dbi)
		defer scanner.Close()
		scanner.Set(fk, nil, lmdb.SetRange)
		for scanner.Scan() {
			key := scanner.Key()
			val := scanner.Val()
			if inc {
				if bytes.Compare(key, lk) > 0 {
					return nil
				}
			} else {
				if bytes.Compare(key, lk) >= 0 {
					return nil
				}
			}
			cont, err := op(key, val)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}
