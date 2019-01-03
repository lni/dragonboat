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

package dragonboat

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/envutil"
)

const (
	// see
	// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
	ec2AZURL           = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
	jsonRegionFileName = "dragonboat-region.json"
)

var (
	// regionUnknown is the string value used to indicate region is unknown.
	regionUnknown = settings.Soft.UnknownRegionName
)

func getRegion(ctx context.Context) (string, error) {
	regionFuncs := []func(context.Context) (string, error){
		ec2Region,
		jsonRegion,
		envRegion,
	}
	for _, f := range regionFuncs {
		if r, err := f(ctx); err == nil && len(r) > 0 {
			return r, nil
		}
	}
	return regionUnknown, nil
}

func ec2Region(ctx context.Context) (string, error) {
	req, err := http.NewRequest("GET", ec2AZURL, nil)
	if err != nil {
		return "", err
	}
	tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(tctx))
	if err != nil {
		return "", err
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			plog.Errorf("failed to close %v", err)
		}
	}()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type jsonRegionRec struct {
	Region string
}

func jsonRegion(ctx context.Context) (string, error) {
	directories := envutil.GetConfigDirs()
	for _, dir := range directories {
		fp := filepath.Join(dir, jsonRegionFileName)
		f, err := os.Open(filepath.Clean(fp))
		if err != nil {
			continue
		}
		data, err := ioutil.ReadAll(f)
		defer func() {
			if err = f.Close(); err != nil {
				plog.Errorf("failed to close file %v", err)
			}
		}()
		if err == nil {
			var jr jsonRegionRec
			err = json.Unmarshal(data, &jr)
			if err == nil {
				return jr.Region, nil
			}
		}
	}
	return "", nil
}

func envRegion(ctx context.Context) (string, error) {
	return envutil.GetStringEnvVarOrEmptyString("DRAGONBOAT_REGION"), nil
}
