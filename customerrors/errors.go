package customerrors

import "errors"

var (
	CheckConfigNilError = errors.New("check config nil")
	CheckDBPoolError    = errors.New("check db pool nil")
)
