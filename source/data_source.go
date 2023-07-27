package source

import jsoniter "github.com/json-iterator/go"

var (
	json = jsoniter.ConfigFastest
)

type DataSource interface {
	GetData() (any, error)
	Progress() (int64, float32)
	Close() error
}
