package logger

import (
	"container/list"
	"fmt"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/mteznja4ma/grodyia/util"
	colorPrint "github.com/mteznja4ma/grodyia/util/colorprint"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

// levels
const (
	TraceLevel   = 0
	DebugLevel   = 1
	InfoLevel    = 2
	WarningLevel = 3
	ErrorLevel   = 4
	FatalLevel   = 5
)

type Logger struct {
	File   string
	Line   int
	Level  int
	LogStr string
	TimeNs int64
}

var (
	contextLogger  *logrus.Entry  = nil
	logDir                        = ""
	screenPrint                   = 1
	MinLevel                      = 0 // logout more than this level
	chanPrint                     = make(chan Logger, 100)
	cb             func(i Logger) = nil
	tmpLogList                    = list.New()
	maxTmpLogCount                = 100000
	mxTemLogList   sync.Mutex

	levelName = map[int]string{
		TraceLevel:   " [ trace ] ",
		DebugLevel:   " [ debug ] ",
		InfoLevel:    " [ info ] ",
		WarningLevel: " [ warning ] ",
		ErrorLevel:   " [ error ] ",
		FatalLevel:   " [ fatal ] ",
	}
)

// init starts the background printer.
func init() {
	go func() {
		for {
			i := <-chanPrint
			logStr := i.LogStr
			if i.Level >= WarningLevel {
				c := colorPrint.FontColor.LightGray
				switch i.Level {
				case WarningLevel:
					c = colorPrint.FontColor.Yellow
				case ErrorLevel:
					c = colorPrint.FontColor.Red
				case FatalLevel:
					c = colorPrint.FontColor.LightRed
				}
				colorPrint.ColorPrint(logStr, c)
				colorPrint.ColorPrint("\n", colorPrint.FontColor.LightGray)
			} else {
				fmt.Println(logStr)
			}
		}
	}()
}

// New initializes the logger and creates the log file.
func New(directory string) error {
	if directory == "" {
		contextLogger = nil
		logDir = ""
		return nil
	}

	contextLogger = logrus.WithFields(logrus.Fields{})
	logDir = directory
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})

	logrus.SetLevel(logrus.TraceLevel)

	filename := time.Now().Format("2006-01-02_15-04-05") + ".log"
	logrus.SetOutput(&lumberjack.Logger{
		Filename:   path.Join(logDir, filename),
		MaxSize:    50, // M
		MaxBackups: 100,
		MaxAge:     90,   //days
		Compress:   true, // disabled by default
		LocalTime:  true,
	})

	Info("logger initialized successfully")
	return nil
}

// SetCallback sets the log callback.
func SetCallback(c func(i Logger)) {
	if cb != nil && c != nil {
		return
	}
	cb = c
	if cb != nil {
		mxTemLogList.Lock()
		for e := tmpLogList.Front(); e != nil; {
			c(e.Value.(Logger))
			e = e.Next()
		}
		tmpLogList.Init()
		mxTemLogList.Unlock()
	}
}

// SetScreenPrint toggles screen output.
func SetScreenPrint(print int) {
	screenPrint = print
}

// nowTimeString returns the current timestamp string.
func nowTimeString() string {
	now := time.Now()
	timeStr := fmt.Sprintf("%v-%02d-%02d %02d:%02d:%02d.%09d",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond())
	return timeStr
}

// TryE recovers from logging panics and writes a dump file.
func TryE(pathName string) {
	errs := recover()
	if errs == nil {
		return
	}

	filename := fmt.Sprintf("%s_pid%d_dump.log",
		time.Now().Format("2006-01-02_15-04-05"),
		os.Getpid())
	f, err := os.Create(path.Join(pathName, filename))
	if err != nil {
		return
	}

	defer f.Close()

	f.WriteString(fmt.Sprintf("%v\r\n", errs)) // panic
	f.WriteString("========\r\n")
	f.WriteString(string(debug.Stack())) // stack
}

// writeLog formats and writes a log entry.
func writeLog(file, format string, line, level int, stack string, a ...any) {
	dir, _ := util.GetCurrentPath()
	dir = path.Join(dir, "log")
	if contextLogger != nil {
		dir = logDir
	}
	defer TryE(dir)
	if level < MinLevel {
		return
	}

	message := fmt.Sprintf(format, a...)
	shortLocation := fmt.Sprintf("%s:%d", path.Base(file), line)
	fullLocation := fmt.Sprintf("%s:%d", file, line)
	logStr := fmt.Sprintf("%s%s[%s] %s", nowTimeString(), GetLogLevelStr(level), shortLocation, message)
	if stack != "" {
		logStr += "\n" + stack
	}

	// merge log
	if screenPrint != 0 || level >= ErrorLevel || cb == nil {
		chanPrint <- Logger{
			LogStr: logStr,
			Level:  level,
		}
	}

	// save all log
	if contextLogger != nil {
		logStr := fmt.Sprintf("%s%s[%s] %s", nowTimeString(), GetLogLevelStr(level), fullLocation, message)
		if stack != "" {
			logStr += "\n" + stack
		}
		switch level {
		case TraceLevel:
			logrus.Trace(logStr)
		case DebugLevel:
			logrus.Debug(logStr)
		case InfoLevel:
			logrus.Info(logStr)
		case WarningLevel:
			logrus.Warning(logStr)
		case ErrorLevel, FatalLevel:
			logrus.Error(logStr)
		default:
			logrus.Info(logStr)
		}
	}

	// save logInfo
	logInfo := Logger{
		File:   file,
		Line:   line,
		Level:  level,
		LogStr: logStr,
		TimeNs: time.Now().UnixNano(),
	}
	if cb != nil {
		cb(logInfo)
	} else {
		mxTemLogList.Lock()
		if tmpLogList.Len() > maxTmpLogCount {
			tmpLogList.Remove(tmpLogList.Front())
		}
		tmpLogList.PushBack(logInfo)
		mxTemLogList.Unlock()
	}
}

// Trace logs a trace-level message.
func Trace(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, TraceLevel, "", a...)
}

// Debug logs a debug-level message.
func Debug(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, DebugLevel, "", a...)
}

// Info logs an info-level message.
func Info(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, InfoLevel, "", a...)
}

// Warning logs a warning-level message.
func Warning(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, WarningLevel, "", a...)
}

// Error logs an error-level message.
func Error(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, ErrorLevel, "", a...)
}

// Fatal logs a fatal-level message and exits.
func Fatal(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	writeLog(file, format, line, FatalLevel, "", a...)
	time.Sleep(time.Second / 2)
	os.Exit(1)
}

// GetLogLevelStr returns the formatted log level label.
func GetLogLevelStr(level int) string {
	if _, ok := levelName[level]; ok {
		return levelName[level]
	}
	return ""
}
