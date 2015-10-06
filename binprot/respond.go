/**
 * Functions to respond in kind to binary-based requests
 */
package binprot

import "bufio"
import "bytes"
import "encoding/binary"

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

type BinaryResponder struct {
    writer *bufio.Writer
}

func NewBinaryResponder(writer *bufio.Writer) BinaryResponder {
    return BinaryResponder {
        writer: writer,
    }
}

// TODO: CAS?
func (b BinaryResponder) Set() error {
    header := makeSuccessResponseHeader(OPCODE_SET, 0, 0, 0, 0)
    return writeHeader(header, b.writer)
}

func (b BinaryResponder) Get(response common.GetResponse) error {
    // total body length = extras (flags, 4 bytes) + data length
    totalBodyLength := len(response.Data) + 4
    header := makeSuccessResponseHeader(OPCODE_GET, 0, 4, totalBodyLength, 0)
    
    err := writeHeader(header, b.writer)
    if err != nil { return err }
    
    err = binary.Write(b.writer, binary.BigEndian, response.Metadata.OrigFlags)
    if err != nil { return err }
    
    _, err = b.writer.Write(response.Data)
    if err != nil { return err }

    b.writer.Flush()
    return nil
}

func (b BinaryResponder) GetMiss(response common.GetResponse) error {
    header := makeErrorResponseHeader(OPCODE_GET, int(STATUS_KEY_ENOENT), 0)
    return writeHeader(header, b.writer)
}

func (b BinaryResponder) GetEnd() error {
    // no-op since the binary protocol does not have batch gets
    return nil
}

func (b BinaryResponder) Delete() error {
    header := makeSuccessResponseHeader(OPCODE_DELETE, 0, 0, 0, 0)
    return writeHeader(header, b.writer)
}

func (b BinaryResponder) Touch() error {
    header := makeSuccessResponseHeader(OPCODE_TOUCH, 0, 0, 0, 0)
    return writeHeader(header, b.writer)
}

func (b BinaryResponder) Error(err error) error {
    // TODO: proper opcode
    header := makeErrorResponseHeader(OPCODE_GET, int(errorToCode(err)), 0)
    return writeHeader(header, b.writer)
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
