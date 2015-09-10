/**
 * Common types and constants
 */
package binaryprot

var MAGIC_REQUEST = 0x80

const OPCODE_GET    = 0x00
const OPCODE_GETQ   = 0x09 // (later)
const OPCODE_GAT    = 0x1d // (later)
const OPCODE_GATQ   = 0x1e // (later)
const OPCODE_SET    = 0x01
const OPCODE_DELETE = 0x04
const OPCODE_TOUCH  = 0x1c
const OPCODE_NOOP   = 0x0a // (later)

const MAGIC_RESPONSE = 0x81

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
    Magic           uint8  // Already known, since we're here
    Opcode          uint8
    KeyLength       uint16
    ExtraLength     uint8
    DataType        uint8  // Always 0
    VBucket         uint16 // Not used
    TotalBodyLength uint32 // Not useful
    OpaqueToken     uint32 // Echoed to the client
    CASToken        uint64 // Unused in current implementation
}

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