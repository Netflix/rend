package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"

	"github.com/netflix/rend/binprot"
)

func isBinaryRequest(reader *bufio.Reader) (bool, error) {
	headerByte, err := reader.Peek(1)
	if err != nil {
		return false, err
	}
	return headerByte[0] == binprot.MagicRequest, nil
}

func abort(toClose []io.Closer, err error) {
	if err != nil && err != io.EOF {
		log.Println("Error while processing request. Closing connection. Error:", err.Error())
	}
	for _, c := range toClose {
		if c != nil {
			c.Close()
		}
	}
}

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	return fmt.Sprintf("Panic occured at: %v:%v (line %v)", file, name, line)
}
