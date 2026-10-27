package test

import (
	"testing"
	"time"

	"github.com/coderyw/goeonid/v2/eonid"
)

// 141999735878713349
// 1759816238261
func TestGetMinimumIdInTimestamp(t *testing.T) {
	t15 := time.Unix(1761753600, 0)
	//t15, _ := time.ParseInLocation(time.DateTime, "2025-09-24 00:00:00", time.Local)
	t.Logf("timestamp=%v\n", t15.Unix())
	t.Logf("timestamp=%v\n", t15.Format("2006-01-02 15:04:05"))
	type args struct {
		timestampS int64
		maxId      bool
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "当前时间min", args: args{timestampS: t15.Unix(), maxId: false}},
		{name: "当前时间max", args: args{timestampS: t15.Unix(), maxId: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := eonid.GetIdInTimestamp(tt.args.timestampS, tt.args.maxId)
			t.Logf("GetMinimumIdInTimestamp() = %v\n", got)
		})
	}
}
