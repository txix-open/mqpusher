package script

import (
	"os"
	"time"

	"github.com/pkg/errors"
	scripts "github.com/txix-open/isp-script"
)

const (
	scriptTimeout = 5 * time.Second
)

type converter struct {
	engine     *scripts.Engine
	execScript scripts.Script
}

func NewConverter(filePath string) (converter, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return converter{}, errors.WithMessagef(err, "read file '%s'", filePath)
	}

	script, err := scripts.NewScript([]byte("(function() {\n"), file, []byte("\n})();"))
	if err != nil {
		return converter{}, errors.WithMessage(err, "new script")
	}

	return converter{
		engine:     scripts.NewEngine(),
		execScript: script,
	}, nil
}

func (c converter) Convert(data any) (any, error) {
	v, err := c.engine.Execute(c.execScript, data,
		scripts.WithTimeout(scriptTimeout),
		scripts.WithFieldNameMapper(jsonFieldNameMapper{}),
		scripts.WithDefaultToolkit(),
		scripts.WithLogger(scripts.NewStdoutJsonLogger()),
	)
	if err != nil {
		return nil, errors.WithMessage(err, "execute script")
	}
	return v, nil
}
