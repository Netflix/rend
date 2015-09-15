package binprot

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"
import "io"

import "../common"

// Example Set Request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x02
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x08
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000012
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//       Expiry     (28-31): 0x00000e10
//     Key          (32-36): The textual string "Hello"
//     Value        (37-41): The textual string "World"

// Example Get request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x00
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005 (for "Hello")
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key          (24-29): The string key (e.g. "Hello")
//     Value               : None

// Example Delete request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : The textual string "Hello"
//     Value               : None

// Example Touch request (not from docs)
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x04
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              :
//       Expiry     (24-27): 0x00000e10
//     Key                 : The textual string "Hello"
//     Value               : None

type BinaryParser struct { }

func (p BinaryParser) ParseRequest(remoteReader *bufio.Reader) (interface{}, common.RequestType, error) {
    
    // read in the full header before any variable length fields
    headerBuf := make([]byte, 24)
    _, err := io.ReadFull(remoteReader, headerBuf)
    
    if err != nil {
        if err == io.EOF {
            fmt.Println("End of file: Connection closed?")
        } else {
            fmt.Println(err.Error())
        }
        return nil, common.REQUEST_GET, err
    }
    
    var reqHeader RequestHeader
    binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &reqHeader)
    
    switch reqHeader.Opcode {
        case OPCODE_SET:
            // flags, exptime, key, value
            flags, err := readUInt32(remoteReader)
            
            if err != nil {
                fmt.Println("Error reading flags")
                return nil, common.REQUEST_SET, err
            }
            
            exptime, err := readUInt32(remoteReader)
            
            if err != nil {
                fmt.Println("Error reading exptime")
                return nil, common.REQUEST_SET, err
            }
            
            key, err := readString(remoteReader, reqHeader.KeyLength)
            
            if err != nil {
                fmt.Println("Error reading key")
                return nil, common.REQUEST_SET, err
            }
            
            realLength := reqHeader.TotalBodyLength -
                            uint32(reqHeader.ExtraLength) -
                            uint32(reqHeader.KeyLength)
            
            return common.SetRequest {
                Key:     key,
                Flags:   flags,
                Exptime: exptime,
                Length:  realLength,
            }, common.REQUEST_SET, nil
            
        case OPCODE_GET:
            // key
            key, err := readString(remoteReader, reqHeader.KeyLength)
            
            if err != nil {
                fmt.Println("Error reading key")
                return nil, common.REQUEST_GET, err
            }
            
            return common.GetRequest {
                Key: key,
            }, common.REQUEST_GET, nil
            
        case OPCODE_DELETE:
            // key
            key, err := readString(remoteReader, reqHeader.KeyLength)
            
            if err != nil {
                fmt.Println("Error reading key")
                return nil, common.REQUEST_DELETE, err
            }
            
            return common.DeleteRequest {
                Key: key,
            }, common.REQUEST_DELETE, nil
            
        case OPCODE_TOUCH:
            // exptime, key
            exptime, err := readUInt32(remoteReader)
            
            if err != nil {
                fmt.Println("Error reading exptime")
                return nil, common.REQUEST_TOUCH, err
            }
            
            key, err := readString(remoteReader, reqHeader.KeyLength)
            
            if err != nil {
                fmt.Println("Error reading key")
                return nil, common.REQUEST_TOUCH, err
            }
            
            return common.TouchRequest {
                Key:     key,
                Exptime: exptime,
            }, common.REQUEST_TOUCH, nil
    }
    
    return nil, common.REQUEST_GET, nil
}

func readString(remoteReader *bufio.Reader, length uint16) ([]byte, error) {
    buf := make([]byte, length)
    _, err := io.ReadFull(remoteReader, buf)
    
    if err != nil { return nil, err }
    
    return buf, nil
}

func readUInt32(remoteReader *bufio.Reader) (uint32, error) {
    var num uint32
    err := binary.Read(remoteReader, binary.BigEndian, &num)
    
    if err != nil { return uint32(0), err }
    
    return num, nil
}
