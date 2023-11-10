package utils

import (
	"fmt"
	"os"
	"strings"
)

func Getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

// SliceToString make [1 2 3] as 1,2,3
func SliceToString(list ...interface{}) string {
	return strings.ReplaceAll(strings.Trim(fmt.Sprint(list), "[]"), " ", ",")
}
