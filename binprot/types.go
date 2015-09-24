/**
 * Common types and constants
 */
package binprot

import "bytes"
import "encoding/binary"
import "io"

import "../common"

var MAGIC_REQUEST = 0x80

// All opcodes as defined in memcached
// Minus SASL and range ops
const OPCODE_GET        = 0x00
const OPCODE_SET        = 0x01
const OPCODE_ADD        = 0x02
const OPCODE_REPLACE    = 0x03
const OPCODE_DELETE     = 0x04
const OPCODE_INCREMENT  = 0x05
const OPCODE_DECREMENT  = 0x06
const OPCODE_QUIT       = 0x07
const OPCODE_FLUSH      = 0x08
const OPCODE_GETQ       = 0x09
const OPCODE_NOOP       = 0x0a
const OPCODE_VERSION    = 0x0b
const OPCODE_GETK       = 0x0c
const OPCODE_GETKQ      = 0x0d
const OPCODE_APPEND     = 0x0e
const OPCODE_PREPEND    = 0x0f
const OPCODE_STAT       = 0x10
const OPCODE_SETQ       = 0x11
const OPCODE_ADDQ       = 0x12
const OPCODE_REPLACEQ   = 0x13
const OPCODE_DELETEQ    = 0x14
const OPCODE_INCREMENTQ = 0x15
const OPCODE_DECREMENTQ = 0x16
const OPCODE_QUITQ      = 0x17
const OPCODE_FLUSHQ     = 0x18
const OPCODE_APPENDQ    = 0x19
const OPCODE_PREPENDQ   = 0x1a
const OPCODE_TOUCH      = 0x1c
const OPCODE_GAT        = 0x1d
const OPCODE_GATQ       = 0x1e
const OPCODE_GATK       = 0x23
const OPCODE_GATKQ      = 0x24

const MAGIC_RESPONSE = 0x81

const STATUS_SUCCESS         = uint16(0x00)
const STATUS_KEY_ENOENT      = uint16(0x01)
const STATUS_KEY_EEXISTS     = uint16(0x02)
const STATUS_E2BIG           = uint16(0x03)
const STATUS_EINVAL          = uint16(0x04)
const STATUS_NOT_STORED      = uint16(0x05)
const STATUS_DELTA_BADVAL    = uint16(0x06)
const STATUS_AUTH_ERROR      = uint16(0x20)
const STATUS_AUTH_CONTINUE   = uint16(0x21)
const STATUS_UNKNOWN_COMMAND = uint16(0x81)
const STATUS_ENOMEM          = uint16(0x82)

func DecodeError(header ResponseHeader) error {
    switch header.Status {
        case STATUS_KEY_ENOENT:      return common.ERROR_KEY_NOT_FOUND
        case STATUS_KEY_EEXISTS:     return common.ERROR_KEY_EXISTS
        case STATUS_E2BIG:           return common.ERROR_VALUE_TOO_BIG
        case STATUS_EINVAL:          return common.ERROR_INVALID_ARGS
        case STATUS_NOT_STORED:      return common.ERROR_ITEM_NOT_STORED
        case STATUS_DELTA_BADVAL:    return common.ERROR_BAD_INC_DEC_VALUE
        case STATUS_AUTH_ERROR:      return common.ERROR_AUTH_ERROR
        case STATUS_UNKNOWN_COMMAND: return common.ERROR_UNKNOWN_CMD
        case STATUS_ENOMEM:          return common.ERROR_NO_MEM
    }
    return nil
}

func errorToCode(err error) uint16 {
    switch err {
        case common.ERROR_KEY_NOT_FOUND:      return STATUS_KEY_ENOENT
        case common.ERROR_KEY_EXISTS:         return STATUS_KEY_EEXISTS
        case common.ERROR_VALUE_TOO_BIG:      return STATUS_E2BIG
        case common.ERROR_INVALID_ARGS:       return STATUS_EINVAL
        case common.ERROR_ITEM_NOT_STORED:    return STATUS_NOT_STORED
        case common.ERROR_BAD_INC_DEC_VALUE:  return STATUS_DELTA_BADVAL
        case common.ERROR_AUTH_ERROR:         return STATUS_AUTH_ERROR
        case common.ERROR_UNKNOWN_CMD:        return STATUS_UNKNOWN_COMMAND
        case common.ERROR_NO_MEM:             return STATUS_ENOMEM
    }
    return 0xFFFF
}

const REQ_HEADER_LEN = 24
type RequestHeader struct {
    Magic           uint8  // Already known, since we're here
    Opcode          uint8
    KeyLength       uint16
    ExtraLength     uint8
    DataType        uint8  // Always 0
    VBucket         uint16 // Not used
    TotalBodyLength uint32
    OpaqueToken     uint32 // Echoed to the client
    CASToken        uint64 // Unused in current implementation
}

func MakeRequestHeader(opcode, keyLength, extraLength, totalBodyLength int) RequestHeader {
    return RequestHeader {
        Magic:           uint8(MAGIC_REQUEST),
        Opcode:          uint8(opcode),
        KeyLength:       uint16(keyLength),
        ExtraLength:     uint8(extraLength),
        DataType:        uint8(0),
        VBucket:         uint16(0),
        TotalBodyLength: uint32(totalBodyLength),
        OpaqueToken:     uint32(0),
        CASToken:        uint64(0),
    }
}

func ReadRequestHeader(reader io.Reader) (RequestHeader, error) {
    // read in the full header before any variable length fields
    headerBuf := make([]byte, REQ_HEADER_LEN)
    _, err := io.ReadFull(reader, headerBuf)
    
    if err != nil {
        return RequestHeader{}, err
    }
    
    var reqHeader RequestHeader
    binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &reqHeader)
    
    return reqHeader, nil
}

const RES_HEADER_LEN = 24
type ResponseHeader struct {
    Magic           uint8  // always 0x81
    Opcode          uint8
    KeyLength       uint16
    ExtraLength     uint8
    DataType        uint8  // unused, always 0
    Status          uint16
    TotalBodyLength uint32
    OpaqueToken     uint32 // same as the user passed in
    CASToken        uint64
}

func makeSuccessResponseHeader(opcode, keyLength, extraLength, totalBodyLength, opaqueToken int) ResponseHeader {
    return ResponseHeader {
        Magic:           MAGIC_RESPONSE,
        Opcode:          uint8(opcode),
        KeyLength:       uint16(keyLength),
        ExtraLength:     uint8(extraLength),
        DataType:        uint8(0),
        Status:          uint16(STATUS_SUCCESS),
        TotalBodyLength: uint32(totalBodyLength),
        OpaqueToken:     uint32(opaqueToken),
        CASToken:        uint64(0),
    }
}

func makeErrorResponseHeader(opcode, status, opaqueToken int) ResponseHeader {
    return ResponseHeader {
        Magic:           MAGIC_RESPONSE,
        Opcode:          uint8(opcode),
        KeyLength:       uint16(0),
        ExtraLength:     uint8(0),
        DataType:        uint8(0),
        Status:          uint16(status),
        TotalBodyLength: uint32(0),
        OpaqueToken:     uint32(opaqueToken),
        CASToken:        uint64(0),
    }
}

func ReadResponseHeader(reader io.Reader) (ResponseHeader, error) {
    // read in the full header before any variable length fields
    headerBuf := make([]byte, RES_HEADER_LEN)
    _, err := io.ReadFull(reader, headerBuf)
    
    if err != nil {
        return ResponseHeader{}, err
    }
    
    var resHeader ResponseHeader
    binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &resHeader)
    
    return resHeader, nil
}
