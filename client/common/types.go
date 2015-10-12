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
    Set   (rw io.ReadWriter, key []byte, value []byte) error
    Get   (rw io.ReadWriter, key []byte              ) error
    Delete(rw io.ReadWriter, key []byte              ) error
    Touch (rw io.ReadWriter, key []byte              ) error
}