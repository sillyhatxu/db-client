package utils

import (
	"reflect"
)

func IsStruct(input interface{}) bool {
	inputT := reflect.TypeOf(input)
	return inputT.Kind() == reflect.Struct || IsStructPtr(inputT)
}

func IsSlice(input interface{}) bool {
	inputT := reflect.TypeOf(input)
	return inputT.Kind() == reflect.Slice
}

func IsStructPtr(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
}
