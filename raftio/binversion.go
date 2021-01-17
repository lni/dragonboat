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

package raftio

const (
	// LogDBBinVersion is the current logdb binary compatibility version
	// implemented in Dragonboat.
	// For v1.4  BinVersion = 100
	//     v2.0  BinVersion = 210
	LogDBBinVersion uint32 = 210
	// PlainLogDBBinVersion is the logdb binary compatibility version value when
	// plain entries are used in ILogDB.
	PlainLogDBBinVersion uint32 = 100
	// TransportBinVersion is the transport binary compatibility version implemented in
	// Dragonboat.
	// For v1.4  TransportBinLog = 100
	//     v2.0  TransportBinLog = 210
	TransportBinVersion uint32 = 210
)
