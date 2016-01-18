/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Datatypes used for internal representation of requests
 */
package common

import "errors"

var (
	BAD_LENGTH = errors.New("CLIENT_ERROR length is not a valid integer")
	BAD_FLAGS = errors.New("CLIENT_ERROR flags is not a valid integer")

	ERROR_KEY_NOT_FOUND = errors.New("ERROR Key not found")
	ERROR_KEY_EXISTS = errors.New("ERROR Key already exists")
	ERROR_VALUE_TOO_BIG = errors.New("ERROR Value too big")
	ERROR_INVALID_ARGS = errors.New("ERROR Invalid arguments")
	ERROR_ITEM_NOT_STORED = errors.New("ERROR Item not stored (CAS didn't match)")
	ERROR_BAD_INC_DEC_VALUE = errors.New("ERROR Bad increment/decrement value")
	ERROR_AUTH_ERROR = errors.New("ERROR Authentication error")
	ERROR_UNKNOWN_CMD = errors.New("ERROR Unknown command")
	ERROR_NO_MEM = errors.New("ERROR Out of memory")
)

// Make sure to keep this list in sync with the one above
// It should contain all ERROR_* that could come back from
// memcached itself
func IsAppError(err error) bool {
	return err == ERROR_KEY_NOT_FOUND ||
		err == ERROR_KEY_EXISTS ||
		err == ERROR_VALUE_TOO_BIG ||
		err == ERROR_INVALID_ARGS ||
		err == ERROR_ITEM_NOT_STORED ||
		err == ERROR_BAD_INC_DEC_VALUE ||
		err == ERROR_AUTH_ERROR ||
		err == ERROR_UNKNOWN_CMD ||
		err == ERROR_NO_MEM
}

type RequestType int

const (
	REQUEST_UNKNOWN = iota
	REQUEST_GET
	REQUEST_GETQ
	REQUEST_GAT
	REQUEST_SET
	REQUEST_DELETE
	REQUEST_TOUCH
)

type RequestParser interface {
	Parse() (interface{}, RequestType, error)
}

type Responder interface {
	Set() error
	Get(response GetResponse) error
	GetMiss(response GetResponse) error
	GetEnd(noopEnd bool) error
	GAT(response GetResponse) error
	GATMiss(response GetResponse) error
	Delete() error
	Touch() error
	Error(err error) error
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
	Quiet   []bool
	NoopEnd bool
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

type GATRequest struct {
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
	Quiet    bool
}

const METADATA_SIZE = 32

type Metadata struct {
	Length    uint32
	OrigFlags uint32
	NumChunks uint32
	ChunkSize uint32
	// This size should stay the same as local.TOKEN_SIZE
	Token [16]byte
}
