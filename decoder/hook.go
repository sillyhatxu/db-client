package decoder

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// DecodeHookFunc is the callback function that can be used for
// data transformations. See "DecodeHook" in the DecoderConfig
// struct.
//
// The type should be DecodeHookFuncType or DecodeHookFuncKind.
// Either is accepted. Types are a superset of Kinds (Types can return
// Kinds) and are generally a richer thing to use, but Kinds are simpler
// if you only need those.
//
// The reason DecodeHookFunc is multi-typed is for backwards compatibility:
// we started with Kinds and then realized Types were the better solution,
// but have a promise to not break backwards compat so we now support
// both.
type DecodeHookFunc interface{}

// DecodeHookFuncType is a DecodeHookFunc which has complete information about
// the source and target types.
type DecodeHookFuncType func(reflect.Type, reflect.Type, interface{}) (interface{}, error)

// DecodeHookFuncKind is a DecodeHookFunc which knows only the Kinds of the
// source and target types.
type DecodeHookFuncKind func(reflect.Kind, reflect.Kind, interface{}) (interface{}, error)

// DecodeHookExec executes the given decode hook. This should be used
// since it'll naturally degrade to the older backwards compatible DecodeHookFunc
// that took reflect.Kind instead of reflect.Type.
func DecodeHookExec(
	raw DecodeHookFunc,
	from reflect.Type, to reflect.Type,
	data interface{}) (interface{}, error) {
	switch f := typedDecodeHook(raw).(type) {
	case DecodeHookFuncType:
		return f(from, to, data)
	case DecodeHookFuncKind:
		return f(from.Kind(), to.Kind(), data)
	default:
		return nil, errors.New("invalid decode hook signature")
	}
}

// typedDecodeHook takes a raw DecodeHookFunc (an interface{}) and turns
// it into the proper DecodeHookFunc type, such as DecodeHookFuncType.
func typedDecodeHook(h DecodeHookFunc) DecodeHookFunc {
	// Create variables here so we can reference them with the reflect pkg
	var f1 DecodeHookFuncType
	var f2 DecodeHookFuncKind

	// Fill in the variables into this interface and the rest is done
	// automatically using the reflect package.
	potential := []interface{}{f1, f2}

	v := reflect.ValueOf(h)
	vt := v.Type()
	for _, raw := range potential {
		pt := reflect.ValueOf(raw).Type()
		if vt.ConvertibleTo(pt) {
			return v.Convert(pt).Interface()
		}
	}

	return nil
}

// StringToTimeHookFunc returns a DecodeHookFunc that converts
// strings to time.Time.
func StringToTimeHookFunc(layout string) DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}
		// Convert it by parsing
		return time.Parse(layout, data.(string))
	}
}

// StringToSliceHookFunc returns a DecodeHookFunc that converts
// string to []string by splitting on the given sep.
func StringToSliceHookFunc(sep string) DecodeHookFunc {
	return func(
		f reflect.Kind,
		t reflect.Kind,
		data interface{}) (interface{}, error) {
		if f != reflect.String || t != reflect.Slice {
			return data, nil
		}

		raw := data.(string)
		if raw == "" {
			return []string{}, nil
		}

		return strings.Split(raw, sep), nil
	}
}

// StringToTimeDurationHookFunc returns a DecodeHookFunc that converts
// strings to time.Duration.
func StringToTimeDurationHookFunc() DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Duration(5)) {
			return data, nil
		}

		// Convert it by parsing
		return time.ParseDuration(data.(string))
	}
}

// WeaklyTypedHook is a DecodeHookFunc which adds support for weak typing to
// the decoder.
// Note that this is significantly different from the WeaklyTypedInput option
// of the DecoderConfig.
func WeaklyTypedHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	dataVal := reflect.ValueOf(data)
	switch t {
	case reflect.String:
		switch f {
		case reflect.Bool:
			if dataVal.Bool() {
				return "1", nil
			}
			return "0", nil
		case reflect.Float32:
			return strconv.FormatFloat(dataVal.Float(), 'f', -1, 64), nil
		case reflect.Int:
			return strconv.FormatInt(dataVal.Int(), 10), nil
		case reflect.Slice:
			dataType := dataVal.Type()
			elemKind := dataType.Elem().Kind()
			if elemKind == reflect.Uint8 {
				return string(dataVal.Interface().([]uint8)), nil
			}
		case reflect.Uint:
			return strconv.FormatUint(dataVal.Uint(), 10), nil
		}
	}
	return data, nil
}
