/**
 * Datatypes used for internal representation of requests
 */
package common

import "bufio"
import "errors"

const verbose = false

// Chunk size, leaving room for the token
const CHUNK_SIZE = 1024 - 16
const FULL_DATA_SIZE = 1024

var MISS       error
var BAD_LENGTH error
var BAD_FLAGS  error

type RequestType int
const REQUEST_GET = RequestType(0)
const REQUEST_SET = RequestType(1)
const REQUEST_DELETE = RequestType(2)
const REQUEST_TOUCH = RequestType(3)

type RequestParser interface {
    ParseRequest(remoteReader *bufio.Reader) (interface{}, RequestType, error)
}

type Responder interface {
    RespondSet(err error, remoteWriter *bufio.Writer) error
    RespondGetChunk(response GetResponse, remoteWriter *bufio.Writer) error
    RespondGetEnd(remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error
    RespondDelete(err error, remoteWriter *bufio.Writer) error
    RespondTouch(err error, remoteWriter *bufio.Writer) error
}

type SetRequest struct {
	Key     []byte
	Flags   uint32
	Exptime uint32
	Length  uint32
    Opaque  uint32
    Data    []byte
}

type GetRequest struct {
	Key    []byte
    Opaque uint32
}

type DeleteRequest struct {
	Key    []byte
    Opaque uint32
}

type TouchRequest struct {
	Key     []byte
	Exptime uint32
    Opaque  uint32
}

type GetResponse struct {
    Key      []byte
    Opaque   uint32
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
