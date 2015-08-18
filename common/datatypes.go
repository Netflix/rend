/**
 * Datatypes used for internal representation of requests
 */
package common

import "errors"

const verbose = false

// Chunk size, leaving room for the token
const CHUNK_SIZE = 1024 - 16
const FULL_DATA_SIZE = 1024

var MISS error
var BAD_LENGTH error
var BAD_FLAGS error
var NOT_FOUND error

const (
    SET = iota
    GET
    TOUCH
    DELETE
)

type Command struct {
    Operation int
    CmdLine *interface{}
}

type SetCmdLine struct {
	cmd     string
	key     string
	flags   int
	exptime string
	length  int
}

type GetCmdLine struct {
	cmd  string
	keys []string
}

type DeleteCmdLine struct {
	cmd string
	key string
}

type TouchCmdLine struct {
	cmd     string
	key     string
	exptime string
}

type GetResponse struct {
    Metadata Metadata
    Data []byte
}

const METADATA_SIZE = 32

type Metadata struct {
	Length    int32
	OrigFlags int32
	NumChunks int32
	ChunkSize int32
	Token     [16]byte
}

func init() {
    // Internal errors
    MISS = errors.New("Cache miss")
    
    // External errors
    BAD_LENGTH = errors.New("CLIENT_ERROR length is not a valid integer")
    BAD_FLAGS = errors.New("CLIENT_ERROR flags is not a valid integer")
    NOT_FOUND = errors.New("NOT_FOUND")
}
