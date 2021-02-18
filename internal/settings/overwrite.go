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

package settings

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
)

func getParsedConfig(fn string) map[string]interface{} {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return nil
	}
	m := map[string]interface{}{}
	b, err := os.ReadFile(filepath.Clean(fn))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	return m
}

func overwriteHardSettings(org *hard) {
	cfg := getParsedConfig("dragonboat-hard-settings.json")
	rd := reflect.Indirect(reflect.ValueOf(org))
	overwriteSettings(cfg, rd)
}

func overwriteSoftSettings(org *soft) {
	cfg := getParsedConfig("dragonboat-soft-settings.json")
	rd := reflect.Indirect(reflect.ValueOf(org))
	overwriteSettings(cfg, rd)
}

func overwriteSettings(cfg map[string]interface{}, rd reflect.Value) {
	for key, val := range cfg {
		field := rd.FieldByName(key)
		if field.IsValid() {
			switch field.Type().String() {
			case "uint64":
				nv := uint64(val.(float64))
				plog.Infof("Setting %s to uint64 value %d", key, nv)
				field.SetUint(nv)
			case "bool":
				plog.Infof("Setting %s to bool value %t", key, val.(bool))
				field.SetBool(val.(bool))
			case "string":
				plog.Infof("Setting %s to string value %s", key, val.(string))
				field.SetString(val.(string))
			default:
			}
		}
	}
}
