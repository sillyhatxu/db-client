package decoder

import (
	"reflect"
	"testing"
	"time"
)

func TestStringToSliceHookFunc(t *testing.T) {
	f := StringToSliceHookFunc(",")

	strType := reflect.TypeOf("")
	sliceType := reflect.TypeOf([]byte(""))
	cases := []struct {
		f, t   reflect.Type
		data   interface{}
		result interface{}
		err    bool
	}{
		{sliceType, sliceType, 42, 42, false},
		{strType, strType, 42, 42, false},
		{
			strType,
			sliceType,
			"foo,bar,baz",
			[]string{"foo", "bar", "baz"},
			false,
		},
		{
			strType,
			sliceType,
			"",
			[]string{},
			false,
		},
	}

	for i, tc := range cases {
		actual, err := DecodeHookExec(f, tc.f, tc.t, tc.data)
		if tc.err != (err != nil) {
			t.Fatalf("case %d: expected err %#v", i, tc.err)
		}
		if !reflect.DeepEqual(actual, tc.result) {
			t.Fatalf(
				"case %d: expected %#v, got %#v",
				i, tc.result, actual)
		}
	}
}

func TestStringToTimeDurationHookFunc(t *testing.T) {
	f := StringToTimeDurationHookFunc()

	strType := reflect.TypeOf("")
	timeType := reflect.TypeOf(time.Duration(5))
	cases := []struct {
		f, t   reflect.Type
		data   interface{}
		result interface{}
		err    bool
	}{
		{strType, timeType, "5s", 5 * time.Second, false},
		{strType, timeType, "5", time.Duration(0), true},
		{strType, strType, "5", "5", false},
	}

	for i, tc := range cases {
		actual, err := DecodeHookExec(f, tc.f, tc.t, tc.data)
		if tc.err != (err != nil) {
			t.Fatalf("case %d: expected err %#v", i, tc.err)
		}
		if !reflect.DeepEqual(actual, tc.result) {
			t.Fatalf(
				"case %d: expected %#v, got %#v",
				i, tc.result, actual)
		}
	}
}

func TestStringToTimeHookFunc(t *testing.T) {
	strType := reflect.TypeOf("")
	timeType := reflect.TypeOf(time.Time{})
	cases := []struct {
		f, t   reflect.Type
		layout string
		data   interface{}
		result interface{}
		err    bool
	}{
		{strType, timeType, time.RFC3339, "2006-01-02T15:04:05Z",
			time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), false},
		{strType, timeType, time.RFC3339, "5", time.Time{}, true},
		{strType, strType, time.RFC3339, "5", "5", false},
	}

	for i, tc := range cases {
		f := StringToTimeHookFunc(tc.layout)
		actual, err := DecodeHookExec(f, tc.f, tc.t, tc.data)
		if tc.err != (err != nil) {
			t.Fatalf("case %d: expected err %#v", i, tc.err)
		}
		if !reflect.DeepEqual(actual, tc.result) {
			t.Fatalf(
				"case %d: expected %#v, got %#v",
				i, tc.result, actual)
		}
	}
}

func TestWeaklyTypedHook(t *testing.T) {
	var f DecodeHookFunc = WeaklyTypedHook

	boolType := reflect.TypeOf(true)
	strType := reflect.TypeOf("")
	sliceType := reflect.TypeOf([]byte(""))
	cases := []struct {
		f, t   reflect.Type
		data   interface{}
		result interface{}
		err    bool
	}{
		// TO STRING
		{
			boolType,
			strType,
			false,
			"0",
			false,
		},

		{
			boolType,
			strType,
			true,
			"1",
			false,
		},

		{
			reflect.TypeOf(float32(1)),
			strType,
			float32(7),
			"7",
			false,
		},

		{
			reflect.TypeOf(int(1)),
			strType,
			int(7),
			"7",
			false,
		},

		{
			sliceType,
			strType,
			[]uint8("foo"),
			"foo",
			false,
		},

		{
			reflect.TypeOf(uint(1)),
			strType,
			uint(7),
			"7",
			false,
		},
	}

	for i, tc := range cases {
		actual, err := DecodeHookExec(f, tc.f, tc.t, tc.data)
		if tc.err != (err != nil) {
			t.Fatalf("case %d: expected err %#v", i, tc.err)
		}
		if !reflect.DeepEqual(actual, tc.result) {
			t.Fatalf(
				"case %d: expected %#v, got %#v",
				i, tc.result, actual)
		}
	}
}
