/**
 * Datatypes used for internal representation of requests
 */
package common

import "bufio"
import "errors"

var (
    MISS       error
    BAD_LENGTH error
    BAD_FLAGS  error
    
    ERROR_KEY_NOT_FOUND     error
    ERROR_KEY_EXISTS        error
    ERROR_VALUE_TOO_BIG     error
    ERROR_INVALID_ARGS      error
    ERROR_ITEM_NOT_STORED   error
    ERROR_BAD_INC_DEC_VALUE error
    ERROR_AUTH_ERROR        error
    ERROR_UNKNOWN_CMD       error
    ERROR_NO_MEM            error
)

type RequestType int
const (
    REQUEST_GET = RequestType(0)
    REQUEST_SET = RequestType(1)
    REQUEST_DELETE = RequestType(2)
    REQUEST_TOUCH = RequestType(3)
)

type RequestParser interface {
    ParseRequest(remoteReader *bufio.Reader) (interface{}, RequestType, error)
}

type Responder interface {
    RespondSet(err error, remoteWriter *bufio.Writer) error
    RespondGetChunk(response GetResponse, remoteWriter *bufio.Writer) error
    RespondGetChunkMiss(response GetResponse, remoteWriter *bufio.Writer) error
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
}

// Gets are batch by default
type GetRequest struct {
	Keys    [][]byte
    Opaques []uint32
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
    Miss     bool
    Key      []byte
    Opaque   uint32
    Metadata Metadata
    Data     []byte
}

const METADATA_SIZE = 32
type Metadata struct {
	Length    uint32
	OrigFlags uint32
	NumChunks uint32
	ChunkSize uint32
	Token     [16]byte
}

func init() {
    // Internal errors
    MISS = errors.New("Cache miss")
    
    // External errors
    BAD_LENGTH = errors.New("CLIENT_ERROR length is not a valid integer")
    BAD_FLAGS = errors.New("CLIENT_ERROR flags is not a valid integer")
    
    ERROR_KEY_NOT_FOUND = errors.New("ERROR")
    ERROR_KEY_EXISTS = errors.New("ERROR")
    ERROR_VALUE_TOO_BIG = errors.New("ERROR")
    ERROR_INVALID_ARGS = errors.New("ERROR")
    ERROR_ITEM_NOT_STORED = errors.New("ERROR")
    ERROR_BAD_INC_DEC_VALUE = errors.New("ERROR")
    ERROR_AUTH_ERROR = errors.New("ERROR")
    ERROR_UNKNOWN_CMD = errors.New("ERROR")
    ERROR_NO_MEM = errors.New("ERROR")
}
