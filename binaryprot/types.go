/**
 * Common types and constants
 */

var HeaderByte = []byte{0x80}

const OPCODE_GET    = 0x00
const OPCODE_GETQ   = 0x09 // (later)
const OPCODE_GAT    = 0x1d // (later)
const OPCODE_GATQ   = 0x1e // (later)
const OPCODE_SET    = 0x01
const OPCODE_DELETE = 0x04
const OPCODE_TOUCH  = 0x1c
const OPCODE_NOOP   = 0x0a // (later)

const RESPONSE_MAGIC = 0x81

const STATUS_SUCCESS         = 0x00
const STATUS_KEY_ENOENT      = 0x01
const STATUS_KEY_EEXISTS     = 0x02
const STATUS_E2BIG           = 0x03
const STATUS_EINVAL          = 0x04
const STATUS_NOT_STORED      = 0x05
const STATUS_DELTA_BADVAL    = 0x06
const STATUS_AUTH_ERROR      = 0x20
const STATUS_AUTH_CONTINUE   = 0x21
const STATUS_UNKNOWN_COMMAND = 0x81
const STATUS_ENOMEM          = 0x82

type RequestHeader struct {
    Magic           int8  // Already known, since we're here
    Opcode          int8
    KeyLength       int16
    ExtraLength     int8
    DataType        int8  // Always 0
    VBucket         int16 // Not used
    TotalBodyLength int32 // Not useful
    OpaqueToken     int32 // Echoed to the client
    CASToken        int64 // Unused in current implementation
}

type ResponseHeader struct {
    Magic           int8  // always 0x81
    Opcode          int8
    KeyLength       int16
    ExtraLength     int8
    DataType        int8  // unused, always 0
    Status          int16
    TotalBodyLength int32
    OpaqueToken     int32 // same as the user passed in
    CASToken        int64
}