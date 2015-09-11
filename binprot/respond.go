/**
 * Functions to respond in kind to binary-based requests
 */
package binprot

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"

import "../common"

// Sample Get response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x00
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x04
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000009
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x00000000000001234
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//     Key                 : None
//     Value        (28-32): The textual string "World"

// Sample Set response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x02
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000001234
//     Extras              : None
//     Key                 : None
//     Value               : None

// Sample Delete response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : None
//     Value               : None

// Sample Touch response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x1c
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : None
//     Value               : None

type BinaryResponder struct { }

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

func errorToCode(err error) int {
    return STATUS_EINVAL
}

func writeHeader(header ResponseHeader, remoteWriter *bufio.Writer) error {
    headerBuf := new(bytes.Buffer)
    binary.Write(headerBuf, binary.BigEndian, header)
    
    // TODO: Error handling for less bytes
    _, err := remoteWriter.Write(headerBuf.Bytes())
    if err != nil { return err }
    
    err = remoteWriter.Flush()
    if err != nil { return err }
    
    return nil
}

// TODO: CAS?
func (r BinaryResponder) RespondSet(err error, remoteWriter *bufio.Writer) error {
    var header ResponseHeader
    
    if err != nil {
        header = makeSuccessResponseHeader(OPCODE_SET, 0, 0, 0, 0)
    } else {
        header = makeErrorResponseHeader(OPCODE_SET, errorToCode(err), 0)
    }
    
    return writeHeader(header, remoteWriter)
}

func (r BinaryResponder) RespondGetChunk(response common.GetResponse, remoteWriter *bufio.Writer) error {
    var header ResponseHeader
    
    // total body length = extras (flags, 4 bytes) + data length
    totalBodyLength := len(response.Data) + 4
    
    header = makeSuccessResponseHeader(OPCODE_GET, 0, 4, totalBodyLength, 0)
    
    err := writeHeader(header, remoteWriter)
    if err != nil { return err }
    
    err = binary.Write(remoteWriter, binary.BigEndian, response.Metadata.OrigFlags)
    if err != nil { return err }
    
    _, err = remoteWriter.Write(response.Data)
    if err != nil { return err }

    remoteWriter.Flush()
    return nil
}

func (r BinaryResponder) RespondGetEnd(remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    // no-op since the binary protocol does not have batch gets
    return nil
}

func (r BinaryResponder) RespondDelete(err error, remoteWriter *bufio.Writer) error {
    var header ResponseHeader
    
    if err != nil {
        header = makeSuccessResponseHeader(OPCODE_DELETE, 0, 0, 0, 0)
    } else {
        header = makeErrorResponseHeader(OPCODE_DELETE, errorToCode(err), 0)
    }
    
    return writeHeader(header, remoteWriter)
}

func (r BinaryResponder) RespondTouch(err error, remoteWriter *bufio.Writer) error {
    var header ResponseHeader
    
    if err != nil {
        header = makeSuccessResponseHeader(OPCODE_TOUCH, 0, 0, 0, 0)
    } else {
        header = makeErrorResponseHeader(OPCODE_TOUCH, errorToCode(err), 0)
    }
    
    return writeHeader(header, remoteWriter)
}

func respondError(err error, remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    if err == common.MISS {
        _, err = fmt.Fprintf(remoteWriter, "NOT_FOUND\r\n")
    } else {
        fmt.Println(err.Error())
        _, err = fmt.Fprintf(remoteWriter, "ERROR\r\n")
    }
    
    if err != nil { return err }
    remoteWriter.Flush()
    return nil
}

