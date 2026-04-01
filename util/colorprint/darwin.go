//go:build darwin
// +build darwin

package colorprint

import "fmt"

var (
	FontColor Color = Color{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
)

type Color struct {
	Black       int // black
	Blue        int // blue
	Green       int // green
	Cyan        int // cyan
	Red         int // red
	Purple      int // purple
	Yellow      int // yellow
	LightGray   int // light gray
	Gray        int // gray
	LightBlue   int // light blue
	LightGreen  int // light green
	LightCyan   int // light cyan
	LightRed    int // light red
	LightPurple int // light purple
	LightYellow int // light yellow
	White       int // white
}

// ColorPrint prints a string with the given color.
func ColorPrint(s string, i int) {
	switch i {
	case FontColor.Yellow:
		fmt.Printf("%c[0;40;33m%s%c[0m", 0x1B, s, 0x1B)
	case FontColor.Red:
		fmt.Printf("%c[0;40;31m%s%c[0m", 0x1B, s, 0x1B)
	case FontColor.LightRed:
		fmt.Printf("%c[1;40;31m%s%c[0m", 0x1B, s, 0x1B)
	default:
		fmt.Print(s)
	}
}
