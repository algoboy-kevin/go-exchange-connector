package utils

import (
	"fmt"
	"time"
)

func SafeParseTimestamp(ts string) time.Time {
	if ts == "" {
		return time.Now()
	}

	millis, err := parseInt64(ts)
	if err != nil {
		return time.Now()
	}

	return time.UnixMilli(millis)
}

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
