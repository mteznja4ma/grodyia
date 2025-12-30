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

/**
 * 初始化日志
 **/
func init() {
	go func() {
		for {
			i := <-chanPrint
			logStr := i.LogStr
			if i.Level >= WarningLevel {
				c := colorPrint.FontColor.LightGray
				if i.Level == WarningLevel {
					c = colorPrint.FontColor.Yellow
				} else if i.Level == ErrorLevel {
					c = colorPrint.FontColor.Red
				} else if i.Level == FatalLevel {
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

// New
/*
 * 创建日志文件
 * @param [directory] string
 * @return (error)
 */
func New(directory string) error {
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

	Info("", "Logger is successfully initialized!")
	return nil
}

// SetCallback
/*
 * 设置日志回调
 * @param [c] func(i LogInfo)
 * @return (error)
 */
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

// SetScreenPrint
/*
 * 日志快照
 * @param [print] int
 */
func SetScreenPrint(print int) {
	screenPrint = print
}

/**
 * 当前时间字符串
 * @return (string)
 **/
func nowTimeString() string {
	now := time.Now()
	timeStr := fmt.Sprintf("%v-%02d-%02d %02d:%02d:%02d.%09d",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond())
	return timeStr
}

// TryE
/*
 * 日志异常捕捉
 * @param [pathName] string
 */
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

/**
 * 日志输出
 * @param [file] 文件
 * @param [format] 格式
 * @param [line] 错误行数
 * @param [level] 日志等级
 * @param [a] 参数
 *
 **/
func printLog(file, format string, line, level int, a ...any) {
	dir, _ := util.GetCurrentPath()
	dir = path.Join(dir, "log")
	if contextLogger != nil {
		dir = logDir
	}
	defer TryE(dir)
	if level < MinLevel {
		return
	}

	// merge log
	if screenPrint != 0 || level >= ErrorLevel || cb == nil {
		logStr := fmt.Sprintf(nowTimeString()+GetLogLevelStr(level)+format, a...)
		chanPrint <- Logger{
			LogStr: logStr,
			Level:  level,
		}
	}

	// save all log
	if contextLogger != nil {
		logStr := fmt.Sprintf(nowTimeString()+" "+format, a...) + fmt.Sprintf(" << %s, line #%d ", file, line)
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
		LogStr: fmt.Sprintf(format, a...),
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

// Trace
/*
 * 追踪级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Trace(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, TraceLevel, a...)
}

// Debug
/*
 * 调试级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Debug(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, DebugLevel, a...)
}

// Info
/*
 * 正常级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Info(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, InfoLevel, a...)
}

// Warning
/*
 * 警告级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Warning(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, WarningLevel, a...)
}

// Error
/*
 * 错误级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Error(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, ErrorLevel, a...)
}

// Fatal
/*
 * 失败级别
 * @param [format] 格式
 * @param [a] 参数
 */
func Fatal(format string, a ...any) {
	_, file, line, _ := runtime.Caller(2)
	printLog(file, format, line, FatalLevel, a...)
	time.Sleep(time.Second / 2)
	os.Exit(1)
}

// GetLogLevelStr
/*
 * 获取日志等级
 * @param [level] int
 * @return (string)
 */
func GetLogLevelStr(level int) string {
	if _, ok := levelName[level]; ok {
		return levelName[level]
	}
	return ""
}
