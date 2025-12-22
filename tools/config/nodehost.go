package config

import (
	"github.com/lni/dragonboat/v4/config"
)

var NHC *config.NodeHostConfig
var RNC *config.Config

var SnapshotSwitch bool

func SetNodeHostConfig(hc *config.NodeHostConfig) {
	NHC = hc
}

func SetRNodeConfig(hc *config.Config) {
	RNC = hc
}

func SetSnapshotSwitch(sw bool) {
	SnapshotSwitch = sw
}
