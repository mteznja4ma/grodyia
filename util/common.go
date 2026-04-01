package util

import (
	"bytes"
	"math/rand/v2"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CurrentMalloc returns the current allocated memory size in KB.
func CurrentMalloc() int64 {
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	return int64(rtm.Alloc / 1024)
}

// ParseArgumentsUint32 returns the uint32 value for a key from key=value args.
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
	return 0, false
}

// ParseArgumentsString returns the string value for a key from key=value args.
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
	return "", false
}

// GetIPFromIPAddress returns the IP part from an address string.
func GetIPFromIPAddress(address string) string {
	a := strings.Split(address, ":")
	if len(a) != 2 {
		return ""
	}
	return a[0]
}

// GetPortFromIPAddress returns the port part from an address string.
func GetPortFromIPAddress(address string) int {
	a := strings.Split(address, ":")
	if len(a) != 2 {
		return 0
	}
	p, _ := strconv.Atoi(a[1])
	return p
}

// RandByte returns a random byte slice with the given length.
func RandByte(length int) []byte {
	var chars = []byte{'.', '/', '?', '%', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}
	buffer := bytes.Buffer{}
	clength := len(chars)
	// Use the global random generator, which is auto-seeded in Go 1.20+.
	for range length {
		buffer.WriteByte(chars[rand.IntN(clength)])
	}
	return buffer.Bytes()
}

// GetUUID returns a new UUID string.
func GetUUID() string {
	return uuid.New().String()
}

// CheckPortUsage reports whether the given local port is in use.
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
