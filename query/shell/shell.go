package main

import (
	"fmt"

	"github.com/pkg/term"
	// "github.com/tylerchr/parallel-database/query/parser"
)

func main() {

	history := make([][]byte, 32)
	historyIdx := 0

	buffer := make([]byte, 1024)
	bufferIdx := 0

	for {

		ascii, keyCode, err := getChar()
		if err == nil {
			if ascii == 3 {
				break
			} else if keyCode == 38 {
				fmt.Printf("\r")
				// fmt.Printf("up arrow\r\n")
			} else if keyCode == 40 {
				fmt.Printf("down arrow\r\n")
			} else if keyCode == 37 {
				fmt.Printf("\b")
				bufferIdx--
				// } else if keyCode == 39 {
				// fmt.Printf("\b")
			} else if ascii == 13 {
				fmt.Printf("\r\n")
				fmt.Printf("%s\r\n", buffer)
				history[historyIdx] = buffer
				buffer = make([]byte, 1024)
			} else if ascii == 127 {
				fmt.Printf("\b \b")
				bufferIdx--
				buffer[bufferIdx] = byte(0)
			} else if ascii != 0 {
				fmt.Printf("%c", ascii)
				buffer[bufferIdx] = byte(ascii)
				bufferIdx++
			} else {
				fmt.Printf("%c (%d %d %v)\r\n", ascii, ascii, keyCode, err)
			}
		}

	}

}

// Returns either an ascii code, or (if input is an arrow) a Javascript key code.
func getChar() (ascii int, keyCode int, err error) {
	t, _ := term.Open("/dev/tty")
	term.RawMode(t)
	bytes := make([]byte, 3)

	var numRead int
	numRead, err = t.Read(bytes)
	if err != nil {
		return
	}
	if numRead == 3 && bytes[0] == 27 && bytes[1] == 91 {
		// Three-character control sequence, beginning with "ESC-[".

		// Since there are no ASCII codes for arrow keys, we use
		// Javascript key codes.
		if bytes[2] == 65 {
			// Up
			keyCode = 38
		} else if bytes[2] == 66 {
			// Down
			keyCode = 40
		} else if bytes[2] == 67 {
			// Right
			keyCode = 39
		} else if bytes[2] == 68 {
			// Left
			keyCode = 37
		}
	} else if numRead == 1 {
		ascii = int(bytes[0])
	} else {
		// Two characters read??
	}
	if ascii == 3 {
		fmt.Printf("BREAK")
		t.Write([]byte("\r\n"))
	}
	term.CBreakMode(t)
	t.Restore()
	t.Close()
	return
}
