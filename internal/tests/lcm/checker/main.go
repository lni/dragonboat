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

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/lni/dragonboat/v3/internal/tests/lcm/porcupine"
)

func main() {
	fp := flag.String("path", "", "path of the history file to be checked")
	timeout := flag.Uint64("timeout", 0, "timeout in seconds")
	flag.Parse()
	if len(*fp) == 0 {
		panic("file path not set")
	}
	m := porcupine.GetEtcdModel()
	events := porcupine.ParseJepsenLog(*fp)
	td := time.Duration(*timeout) * time.Second
	if !porcupine.CheckEventsTimeout(m, events, td) {
		panic(fmt.Sprintf("found linearizability violation in %s", *fp))
	}
}
