// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

func checkRoundTrip(e0 versionEdit) error {
	var e1 versionEdit
	buf := new(bytes.Buffer)
	if err := e0.encode(buf); err != nil {
		return errors.Wrap(err, "encode")
	}
	if err := e1.decode(buf); err != nil {
		return errors.Wrap(err, "decode")
	}
	if diff := pretty.Diff(e0, e1); diff != nil {
		return errors.Errorf("%s", strings.Join(diff, "\n"))
	}
	return nil
}

func TestVersionEditRoundTrip(t *testing.T) {
	testCases := []versionEdit{
		// An empty version edit.
		{},
		// A complete version edit.
		{
			nextFileNum: 44,
			deletedFiles: map[deletedFileEntry]*fileMetadata{
				{
					fileNum: 703,
				}: nil,
				{
					fileNum: 704,
				}: nil,
			},
			newFiles: []newFileEntry{
				{
					meta: &fileMetadata{
						fileNum: 805,
					},
				},
				{
					meta: &fileMetadata{
						fileNum: 806,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		if err := checkRoundTrip(tc); err != nil {
			t.Error(err)
		}
	}
}

// TODO:
// crc is now xxhash64, need to update the tset MANIFEST files as well

/*
func TestVersionEditDecode(t *testing.T) {
	testCases := []struct {
		filename     string
		encodedEdits []string
		edits        []VersionEdit
	}{
		// db-stage-1 and db-stage-2 have the same manifest.
		{
			filename: "db-stage-1/MANIFEST-000001",
			encodedEdits: []string{
				"\x02\x00\x03\x02\x04\x00",
			},
			edits: []VersionEdit{
				{
					NextFileNum: 2,
				},
			},
		},
		// db-stage-3 and db-stage-4 have the same manifest.
		{
			filename: "db-stage-3/MANIFEST-000005",
			encodedEdits: []string{
				"\x02\x00",
				"\x02\x04\t\x00\x03\x06\x04\x05d\x00\x04\xda\a\vbar" +
					"\x00\x05\x00\x00\x00\x00\x00\x00\vfoo\x01\x04\x00" +
					"\x00\x00\x00\x00\x00\x03\x05",
			},
			edits: []VersionEdit{
				{},
				{
					NextFileNum: 6,
					NewFiles: []NewFileEntry{
						{
							Meta: &FileMetadata{
								FileNum: 4,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			f, err := os.Open("./testdata/" + tc.filename)
			if err != nil {
				t.Fatalf("filename=%q: open error: %v", tc.filename, err)
			}
			defer f.Close()
			i, r := 0, NewReader(f, 0)
			for {
				rr, err := r.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("filename=%q i=%d: record reader error: %v", tc.filename, i, err)
				}
				if i >= len(tc.edits) {
					t.Fatalf("filename=%q i=%d: too many version edits", tc.filename, i+1)
				}

				encodedEdit, err := ioutil.ReadAll(rr)
				if err != nil {
					t.Fatalf("filename=%q i=%d: read error: %v", tc.filename, i, err)
				}
				if s := string(encodedEdit); s != tc.encodedEdits[i] {
					t.Fatalf("filename=%q i=%d: got encoded %q, want %q", tc.filename, i, s, tc.encodedEdits[i])
				}

				var edit VersionEdit
				err = edit.Decode(bytes.NewReader(encodedEdit))
				if err != nil {
					t.Fatalf("filename=%q i=%d: decode error: %v", tc.filename, i, err)
				}
				if !reflect.DeepEqual(edit, tc.edits[i]) {
					t.Fatalf("filename=%q i=%d: decode\n\tgot  %#v\n\twant %#v\n%s", tc.filename, i, edit, tc.edits[i],
						strings.Join(pretty.Diff(edit, tc.edits[i]), "\n"))
				}
				if err := checkRoundTrip(edit); err != nil {
					t.Fatalf("filename=%q i=%d: round trip: %v", tc.filename, i, err)
				}

				i++
			}
			if i != len(tc.edits) {
				t.Fatalf("filename=%q: got %d edits, want %d", tc.filename, i, len(tc.edits))
			}
		})
	}
}*/
