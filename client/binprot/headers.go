package binprot

import "encoding/binary"
import "errors"
import "io"

import "../common"

const Get = 0x00
const Set = 0x01
const Add = 0x02
const Replace = 0x03
const Delete = 0x04
const Increment = 0x05
const Decrement = 0x06
const Quit = 0x07
const Flush = 0x08
const GetQ = 0x09
const Noop = 0x0a
const Version = 0x0b
const GetK = 0x0c
const GetKQ = 0x0d
const Append = 0x0e
const Prepend = 0x0f
const Stat = 0x10
const SetQ = 0x11
const AddQ = 0x12
const ReplaceQ = 0x13
const DeleteQ = 0x14
const IncrementQ = 0x15
const DecrementQ = 0x16
const QuitQ = 0x17
const FlushQ = 0x18
const AppendQ = 0x19
const PrependQ = 0x1a
const Verbosity = 0x1b
const Touch = 0x1c
const GAT = 0x1d
const GATQ = 0x1e

type req struct {
    Magic    uint8
    Opcode   uint8
    KeyLen   uint16
    ExtraLen uint8
    DataType uint8
    VBucket  uint16
    BodyLen  uint32
    Opaque   uint32
    CAS      uint64
}

func opToCode(op common.Op) int {
    switch(op) {
        case common.GET:    return Get
        case common.SET:    return Set
        case common.TOUCH:  return Touch
        case common.DELETE: return Delete
        default: return -1
    }
}

func writeReq(w io.Writer, op common.Op, keylen, extralen, bodylen int) error {
    opcode := opToCode(op)

    req := req {
        Magic: 0x80,
        Opcode: uint8(opcode),
        KeyLen: uint16(keylen),
        ExtraLen: uint8(extralen),
        DataType: 0,
        VBucket: 0,
        BodyLen: uint32(bodylen),
        Opaque: 0,
        CAS: 0,
    }

    return binary.Write(w, binary.BigEndian, req)
}

type res struct {
    Magic    uint8
    Opcode   uint8
    KeyLen   uint16
    ExtraLen uint8
    DataType uint8
    Status   uint16
    BodyLen  uint32
    Opaque   uint32
    CAS      uint64
}

func readRes(r io.Reader) (*res, error) {
    res := new(res)
    err := binary.Read(r, binary.BigEndian, res);
    if err != nil {
        return nil, err
    }
    return res, nil
}

var ERR_KEY_NOT_FOUND error
var ERR_KEY_EXISTS error
var ERR_VAL_TOO_LARGE error
var ERR_INVALID_ARGS error
var ERR_ITEM_NOT_STORED error
var ERR_INC_DEC_INVAL error
var ERR_VBUCKET error
var ERR_AUTH error
var ERR_AUTH_CONT error
var ERR_UNKNOWN_CMD error
var ERR_NO_MEM error
var ERR_NOT_SUPPORTED error
var ERR_INTERNAL error
var ERR_BUSY error
var ERR_TEMP error

func init() {
    ERR_KEY_NOT_FOUND = errors.New("Key not found")
    ERR_KEY_EXISTS = errors.New("Key exists")
    ERR_VAL_TOO_LARGE = errors.New("Value too large")
    ERR_INVALID_ARGS = errors.New("Invalid arguments")
    ERR_ITEM_NOT_STORED = errors.New("Item not stored")
    ERR_INC_DEC_INVAL = errors.New("Incr/Decr on non-numeric value.")
    ERR_VBUCKET = errors.New("The vbucket belongs to another server")
    ERR_AUTH = errors.New("Authentication error")
    ERR_AUTH_CONT = errors.New("Authentication continue")
    ERR_UNKNOWN_CMD = errors.New("Unknown command")
    ERR_NO_MEM = errors.New("Out of memory")
    ERR_NOT_SUPPORTED = errors.New("Not supported")
    ERR_INTERNAL = errors.New("Internal error")
    ERR_BUSY = errors.New("Busy")
    ERR_TEMP = errors.New("Temporary failure")
}

func statusToError(status uint16) error {
    switch (status) {
        case uint16(0x01): return ERR_KEY_NOT_FOUND
        case uint16(0x02): return ERR_KEY_EXISTS
        case uint16(0x03): return ERR_VAL_TOO_LARGE
        case uint16(0x04): return ERR_INVALID_ARGS
        case uint16(0x05): return ERR_ITEM_NOT_STORED
        case uint16(0x06): return ERR_INC_DEC_INVAL
        case uint16(0x07): return ERR_VBUCKET
        case uint16(0x08): return ERR_AUTH
        case uint16(0x09): return ERR_AUTH_CONT
        case uint16(0x81): return ERR_UNKNOWN_CMD
        case uint16(0x82): return ERR_NO_MEM
        case uint16(0x83): return ERR_NOT_SUPPORTED
        case uint16(0x84): return ERR_INTERNAL
        case uint16(0x85): return ERR_BUSY
        case uint16(0x86): return ERR_TEMP
    }

    return nil
}

func srsErr(err error) bool {
    switch (err) {
        case ERR_KEY_NOT_FOUND:
        case ERR_KEY_EXISTS:
        case ERR_ITEM_NOT_STORED:
            return false
    }

    return true
}
