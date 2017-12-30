package kinesis

import (
	"fmt"
	"io"
	"os"
	"strings"
)

var DefaultLogger Logger = &Log{w: os.Stdout, err: os.Stderr}

type Log struct {
	w   io.Writer
	err io.Writer
}

func (l *Log) Logf(msg string, args ...interface{}) {
	fmt.Fprintf(l.w, msg+"\n", args...)
}
func (l *Log) Errorf(msg string, args ...interface{}) {
	fmt.Fprintf(l.err, msg, args...)
}

type Logger interface {
	Logf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

type Errors []error

func (e Errors) Error() string {
	msg := ""
	for _, err := range e {
		msg += err.Error() + "\n"
	}
	return strings.TrimSuffix(msg, "\n")
}

type Error string

func (e Error) Error() string { return string(e) }
