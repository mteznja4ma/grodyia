package util

import (
	"bytes"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CurrentMalloc
/*
 * 内存状态
 * @return int64
 */
func CurrentMalloc() int64 {
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	return int64(rtm.Alloc / 1024)
}

// ParseArgumentsUint32
/*
 * 获取键入字符串数组中 key 对应 value
 * @param [name] string - Key
 * @param [args] []string - Maps
 * @return (uint32, bool)
 */
func ParseArgumentsUint32(name string, args []string) (uint32, bool) {
	for _, arg := range args {
		a := strings.Split(arg, "=")
		if len(a) != 2 {
			continue
		}
		if a[0] == name {
			v, err := strconv.Atoi(a[1])
			if err == nil {
				return uint32(v), true
			}
		}
	}
	return 0, true
}

// ParseArgumentsString
/*
 * 获取键入字符串数组中 key 对应 value
 * @param [name] string - Key
 * @param [args] []string - Maps
 * @return (string, bool)
 */
func ParseArgumentsString(name string, args []string) (string, bool) {
	for _, arg := range args {
		a := strings.Split(arg, "=")
		if len(a) != 2 {
			continue
		}
		if a[0] == name {
			return a[1], true
		}
	}
	return "", true
}

// GetIPFromIPAddress
/*
 * 获取IP字符串中Port
 * @param [address] string
 * @return (string)
 */
func GetIPFromIPAddress(address string) string {
	a := strings.Split(address, ":")
	if len(a) != 2 {
		return ""
	}
	return a[0]
}

// GetPortFromIPAddress
/*
 * 获取IP字符串中Port
 * @param [address] string
 * @return (int)
 */
func GetPortFromIPAddress(address string) int {
	a := strings.Split(address, ":")
	if len(a) != 2 {
		return 0
	}
	p, _ := strconv.Atoi(a[1])
	return p
}

// RandByte
/*
 * 获取对应长度的随机字节组
 * @param [length] int
 * @return ([]byte)
 */
func RandByte(length int) []byte {
	var chars = []byte{'.', '/', '?', '%', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}
	buffer := bytes.Buffer{}
	clength := len(chars)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < length; i++ {
		buffer.WriteByte(chars[rand.Intn(clength)])

	}
	return buffer.Bytes()
}

// GetUUID
/*
 * 获取 UUID
 * return (string)
 */
func GetUUID() string {
	return uuid.New().String()
}

// CheckPortUsage
/*
 * 端口占用检测
 * @param [port] int
 * @return (bool)
 */
func CheckPortUsage(port int) bool {
	p := strconv.Itoa(port)
	address := net.JoinHostPort("127.0.0.1", p)
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return false
	}
	defer func() {
		if err := conn.Close(); err != nil {
			return
		}
	}()
	return true
}
