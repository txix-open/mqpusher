package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

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

func UUIDv4() string {
	return uuid.NewString()
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
