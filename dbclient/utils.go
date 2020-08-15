package dbclient

import (
	"strconv"
	"time"
)

func setupBool(input bool) string {
	return strconv.FormatBool(input)
}

func setupInt(input int) string {
	return strconv.Itoa(input)
}

func setupInt64(input int64) string {
	return strconv.FormatInt(input, 10)
}

func setupTime(input time.Duration) string {
	//make sure 1ms<=t<24h
	if input < time.Millisecond || input >= 24*time.Hour {
		return ""
	}
	return input.String()
}
