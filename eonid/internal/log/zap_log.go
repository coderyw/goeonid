// Package log
// @Author: yinwei
// @File: zap_log
// @Version: 1.0.0
// @Date: 2024/9/13 16:21

package log

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Logger *zap.Logger
	once   sync.Once
)

func MsgFmt(msg string) string {
	return fmt.Sprintf("[EnoId] %v", msg)
}

func InitLogger(serverName string) {
	once.Do(func() {
		cfg := zap.NewProductionConfig()
		cfg.Encoding = "console"

		cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoder(func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(fmt.Sprintf("%v %v", t.Format("2006-01-02 15:04:05.000Z07"), t.UnixMilli()))
		})
		cfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
		env := os.Getenv("ENVIRON")
		if env == "" || env == "server-local" {
			cfg.EncoderConfig.EncodeLevel = LowercaseColorLevelEncoder
		} else {
			cfg.EncoderConfig.EncodeLevel = UppercaseColorLevelEncoder
		}
		var err error
		Logger, err = cfg.Build()
		if err != nil {
			Logger = zap.L()
			Logger.Error("Failed to build logger", zap.Error(err))
			return
		}
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.LstdFlags)
		opts := []zap.Option{zap.AddCaller()}
		if len(serverName) > 0 {
			opts = append(opts, zap.Fields(zap.String("appName", serverName)))
		}

		Logger = Logger.WithOptions(opts...)
	})
	return
}

func UppercaseColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(l.CapitalString())
}

func LowercaseColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	//enc.AppendString("\u001B[36mINFO\u001B[0m")

	s, ok := _levelToColor[l]
	if !ok {
		s = _unknownLevelColor
	}
	enc.AppendString(s.add(l.CapitalString()))
}

var _levelToColor = map[zapcore.Level]color{
	zapcore.DebugLevel:  Magenta,
	zapcore.InfoLevel:   Blue,
	zapcore.WarnLevel:   Yellow,
	zapcore.ErrorLevel:  Red,
	zapcore.DPanicLevel: Red,
	zapcore.PanicLevel:  Red,
	zapcore.FatalLevel:  Red,
}
var _unknownLevelColor = Red

type color string

func (c color) add(s string) string {
	return fmt.Sprintf(string(c), 0x1B, s, 0x1B)
}

const (
	Magenta color = "%c[0;42;35m%v%c[0m"
	Cyan    color = "%c[0;39;32m%v%c[0m"
	Test    color = "\033[36m%v\033[0m"
	Blue    color = "%c[0;42;30m%v%c[0m"
	Yellow  color = "%c[0;43;30m%v%c[0m"
	Red     color = "%c[0;41;30m%v%c[0m"
)
