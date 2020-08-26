package script

import (
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

const (
	AwaitTime = 1000 * time.Millisecond
)

var scriptEngine = &Goja{}

func Create(source []byte) (Script, error) {
	script := fmt.Sprintf("%s%s%s", "(function() {", source, "})();")
	return Default().Compile(script)
}

func Default() Engine {
	return scriptEngine
}

type Engine interface {
	Compile(string) (Script, error)
	Execute(Script, interface{}) (interface{}, error)
}

type Script interface {
	Src() interface{}
}

var pool = &sync.Pool{
	New: func() interface{} {
		return initVm()
	},
}

func initVm() *goja.Runtime {
	vm := goja.New()
	vm.SetFieldNameMapper(jsonFieldNameMapper{})

	vm.Set("sha256", Sha256)
	vm.Set("sha512", Sha512)
	vm.Set("time", map[string]interface{}{
		"format": FormatDate,
		"parse":  ParseDate,
	})

	return vm
}

type Goja struct{}

func (*Goja) Compile(script string) (Script, error) {
	prog, err := goja.Compile("script", script, false)
	if err != nil {
		return nil, err
	}
	return &GojaProgram{prog: prog}, nil
}

func (g *Goja) Execute(program Script, arg interface{}) (interface{}, error) {
	value, ok := program.Src().(*goja.Program)
	if !ok {
		return nil, errors.New("unknown engine")
	}

	vm := pool.Get().(*goja.Runtime)
	t := time.AfterFunc(AwaitTime, func() {
		vm.Interrupt("execution timeout")
	})
	defer func() {
		t.Stop()
		vm.ClearInterrupt()
		pool.Put(vm)
	}()

	vm.Set("arg", arg)
	res, err := vm.RunProgram(value)
	if err != nil {
		return nil, g.castErr(err)
	}

	return res.Export(), nil
}

func (*Goja) castErr(err error) error {
	if exception, ok := err.(*goja.Exception); ok {
		val := exception.Value().Export()
		if castedErr, ok := val.(error); ok {
			return castedErr
		}
	}

	return err
}

type GojaProgram struct {
	prog *goja.Program
}

func (a *GojaProgram) Src() interface{} {
	return a.prog
}

func Sha256(value string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(strings.ToLower(value)))
	bytes := hash.Sum(nil)
	return fmt.Sprintf("%x", bytes)
}

func Sha512(value string) string {
	hash := sha512.New()
	_, _ = hash.Write([]byte(strings.ToLower(value)))
	bytes := hash.Sum(nil)
	return fmt.Sprintf("%x", bytes)
}

func FormatDate(t time.Time, format string) string {
	return t.Format(format)
}

func ParseDate(value string, format string) time.Time {
	parse, err := time.Parse(format, value)
	if err != nil {
		panic(err)
	}
	return parse
}

type jsonFieldNameMapper struct{}

//nolint hugeParam
func (jsonFieldNameMapper) FieldName(_ reflect.Type, f reflect.StructField) string {
	list := strings.Split(f.Tag.Get("json"), ",")
	if len(list) > 0 && list[0] != "" {
		return list[0]
	}
	return f.Name
}

func (jsonFieldNameMapper) MethodName(_ reflect.Type, m reflect.Method) string {
	return m.Name
}
