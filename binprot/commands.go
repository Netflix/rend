/**
 * Utility functions to create commands
 */
package binprot

import "bytes"
import "encoding/binary"

func SetCmd(key []byte, flags, exptime, dataSize uint32) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    // key + extras + body
    totalBodyLength := len(key) + 8 + int(dataSize)
    header := MakeRequestHeader(OPCODE_SET, len(key), 8, totalBodyLength)
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, flags)
    binary.Write(reqBuf, binary.BigEndian, exptime)
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
    //return fmt.Sprintf("set %s 0 %s %d\r\n", key, exptime, size)
}

func GetCmd(key []byte) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    header := MakeRequestHeader(OPCODE_GET, len(key), 0, len(key))
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
    //return fmt.Sprintf("get %s\r\n", key)
}

func DeleteCmd(key []byte) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    header := MakeRequestHeader(OPCODE_DELETE, len(key), 0, len(key))
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
    //return fmt.Sprintf("delete %s\r\n", key)
}

func TouchCmd(key []byte, exptime uint32) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    // key + extras + body
    totalBodyLength := len(key) + 4
    header := MakeRequestHeader(OPCODE_TOUCH, len(key), 4, totalBodyLength)
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, exptime)
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
    //return fmt.Sprintf("touch %s %s\r\n", key, exptime)
}
