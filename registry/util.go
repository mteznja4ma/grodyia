package registry

import (
	"strconv"
	"strings"
)

// parseAddr parses address string into host and port
func parseAddr(addr string, defaultPort int) (string, int) {
	parts := strings.Split(addr, ":")
	if len(parts) == 1 {
		return parts[0], defaultPort
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return parts[0], defaultPort
	}
	return parts[0], port
}

