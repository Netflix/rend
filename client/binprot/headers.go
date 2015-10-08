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

func writeReq(writer io.Writer, op common.Op, keylen, extralen, bodylen int) error {
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

    return binary.Write(writer, binary.BigEndian, req)
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

func readRes(reader io.Reader) (res, error) {
    var r res
    err := binary.Read(reader, binary.BigEndian, &r);
    if err != nil {
        return res{}, err
    }
    return r, nil
}

func statusToError(status uint16) error {
    switch (status) {
        case uint16(0x01): return errors.New("Key not found")
        case uint16(0x02): return errors.New("Key exists")
        case uint16(0x03): return errors.New("Value too large")
        case uint16(0x04): return errors.New("Invalid arguments")
        case uint16(0x05): return errors.New("Item not stored")
        case uint16(0x06): return errors.New("Incr/Decr on non-numeric value.")
        case uint16(0x07): return errors.New("The vbucket belongs to another server")
        case uint16(0x08): return errors.New("Authentication error")
        case uint16(0x09): return errors.New("Authentication continue")
        case uint16(0x81): return errors.New("Unknown command")
        case uint16(0x82): return errors.New("Out of memory")
        case uint16(0x83): return errors.New("Not supported")
        case uint16(0x84): return errors.New("Internal error")
        case uint16(0x85): return errors.New("Busy")
        case uint16(0x86): return errors.New("Temporary failure")
    }

    return nil
}
