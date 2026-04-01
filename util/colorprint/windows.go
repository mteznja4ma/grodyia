//go:build windows
// +build windows

package colorprint

import "syscall"

var (
	kernel32    *syscall.LazyDLL  = syscall.NewLazyDLL(`kernel32.dll`)
	proc        *syscall.LazyProc = kernel32.NewProc(`SetConsoleTextAttribute`)
	CloseHandle *syscall.LazyProc = kernel32.NewProc(`CloseHandle`)

	// Initialize the font color palette.
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
	LightGray   int // light gray (system default)
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
	handle, _, _ := proc.Call(uintptr(syscall.Stdout), uintptr(i))
	print(s)
	CloseHandle.Call(handle)
}
