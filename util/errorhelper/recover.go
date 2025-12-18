package errorhelper

import (
	"runtime/debug"

	"github.com/mteznja4ma/grodyia/logger"
)

func Recover() {
	if err := recover(); err != nil {
		logger.Error("ErrorHelper", "Recover Error=[%v], Stack=[%v]\r\n", err, string(debug.Stack()))
	}
}
