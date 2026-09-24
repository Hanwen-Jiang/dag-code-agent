package permissions

import "go.uber.org/zap"

// Logger is the minimal structured logging interface used by the engine.
// go.uber.org/zap.SugaredLogger satisfies this interface directly.
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// NullLogger discards everything.
type NullLogger struct{}

func (NullLogger) Debug(string, ...interface{}) {}
func (NullLogger) Info(string, ...interface{})  {}
func (NullLogger) Warn(string, ...interface{})  {}
func (NullLogger) Error(string, ...interface{}) {}

// ZapLogger adapts a *zap.Logger to Logger.
type ZapLogger struct{ L *zap.Logger }

func NewZapLogger(l *zap.Logger) ZapLogger { return ZapLogger{L: l} }

func (z ZapLogger) Debug(msg string, kv ...interface{}) { z.L.Sugar().Debugw(msg, kv...) }
func (z ZapLogger) Info(msg string, kv ...interface{})  { z.L.Sugar().Infow(msg, kv...) }
func (z ZapLogger) Warn(msg string, kv ...interface{})  { z.L.Sugar().Warnw(msg, kv...) }
func (z ZapLogger) Error(msg string, kv ...interface{}) { z.L.Sugar().Errorw(msg, kv...) }
