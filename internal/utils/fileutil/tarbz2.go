// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package fileutil

import (
	"archive/tar"
	"compress/bzip2"
	"io"
	"os"
	"path/filepath"
)

// ExtractTarBz2 extracts files and directories from the specified tar.bz2 file
// to the specified target directory.
func ExtractTarBz2(bz2fn string, toDir string) error {
	f, err := os.OpenFile(bz2fn, os.O_RDONLY, DefaultFileMode)
	if err != nil {
		return err
	}
	defer f.Close()
	ts := bzip2.NewReader(f)
	tarReader := tar.NewReader(ts)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			target := filepath.Join(toDir, header.Name)
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := func() error {
				fp := filepath.Join(toDir, header.Name)
				nf, err := os.Create(fp)
				if err != nil {
					return err
				}
				defer func() {
					if err := nf.Close(); err != nil {
						panic(err)
					}
				}()
				_, err = io.Copy(nf, tarReader)
				return err
			}(); err != nil {
				return err
			}
		default:
			panic("unknown type")
		}
	}
}
