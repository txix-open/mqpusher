package script

import (
	"reflect"
	"strings"
)

type jsonFieldNameMapper struct{}

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
