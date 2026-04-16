package main

import (
	"errors"
	"fmt"
	"time"
)

var ErrTemporaryFailure = errors.New("temporary payment failure")

func generateID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func getString(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func getFloat(m map[string]any, key string) float64 {
	switch val := m[key].(type) {
	case float64:
		return val
	case int:
		return float64(val)
	default:
		return 0
	}
}

