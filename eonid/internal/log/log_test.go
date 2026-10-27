// Package log
// @Author: yinwei
// @File: log_test
// @Version: 1.0.0
// @Date: 2024/11/28 10:47

package log

import "testing"

func TestInfo(t *testing.T) {
	InitLogger("test")

	Logger.Info("info")
}
