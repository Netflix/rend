package common

import "io"

type Op int

const (
    GET = iota
    SET
    TOUCH
    DELETE
)

type Prot interface {
    Set   (reader io.Reader, writer io.Writer, key []byte, value []byte) error
    Get   (reader io.Reader, writer io.Writer, key []byte              ) error
    Delete(reader io.Reader, writer io.Writer, key []byte              ) error
    Touch (reader io.Reader, writer io.Writer, key []byte              ) error
}