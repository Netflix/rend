/**
 * Datatypes used for internal representation of requests
 */
package common

import "errors"

const verbose = false

// Chunk size, leaving room for the token
const CHUNK_SIZE = 1024 - 16
const FULL_DATA_SIZE = 1024

var MISS       error
var BAD_LENGTH error
var BAD_FLAGS  error

type SetRequest struct {
	Cmd     string
	Key     string
	Flags   int
	Exptime string
	Length  int
}

type GetRequest struct {
	Cmd  string
	Keys []string
}

type DeleteRequest struct {
	Cmd string
	Key string
}

type TouchRequest struct {
	Cmd     string
	Key     string
	Exptime string
}

type GetResponse struct {
    Key      string
    Metadata Metadata
    Data     []byte
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
}
