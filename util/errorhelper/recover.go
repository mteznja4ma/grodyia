package errorhelper

import (
	"runtime/debug"

	"github.com/mteznja4ma/grodyia/logger"
)

// Recover
/*
 * 恢复错误
 */
func Recover() {
	if err := recover(); err != nil {
		logger.Error("Recover Error=[%v], Stack=[%v]\r\n", err, string(debug.Stack()))
	}
}
