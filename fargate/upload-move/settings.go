package main

import (
	"os"
	"strconv"
	"time"
)

const defaultFileMoveTimeout = 60 * time.Minute

func FileMoveTimeout() time.Duration {
	setting, ok := os.LookupEnv("FILE_MOVE_TIMEOUT")
	if ok {
		value, err := strconv.Atoi(setting)
		if err != nil {
			return defaultFileMoveTimeout
		}
		return time.Duration(value) * time.Minute
	} else {
		return defaultFileMoveTimeout
	}
}
