package common

import (
	"context"
	"strings"

	"go.uber.org/zap"
)

const loggerKey = "logger"

func LogInit(logLevel string) {
	var err error
	var logger *zap.Logger
	switch strings.ToLower(logLevel) {
	case "debug":
		logger, err = zap.NewDevelopment()
	default:
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()
}

func WithLog(ctx context.Context) (context.Context, *zap.SugaredLogger) {
	requestLogger := zap.S()
	return context.WithValue(ctx, loggerKey, requestLogger), requestLogger
}

func LogFromContext(ctx context.Context) *zap.SugaredLogger {
	if logger, ok := ctx.Value(loggerKey).(*zap.SugaredLogger); ok {
		return logger
	}
	return zap.S()
}
