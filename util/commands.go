/**
 * Utility functions to create commands
 */
package util

import "bytes"
import "encoding/binary"
import "fmt"

import "../binprot"

// TODO: consider moving into binprot package, seems leaky here
func SetCmd(key []byte, flags, exptime, dataSize uint32) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    // TODO: uint? int?
    totalBodyLength := len(key) + int(dataSize)
    header := binprot.MakeRequestHeader(binprot.OPCODE_SET, len(key), 0, totalBodyLength)
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, flags)
    binary.Write(reqBuf, binary.BigEndian, exptime)
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
	//return fmt.Sprintf("set %s 0 %s %d\r\n", key, exptime, size)
}

func GetCommand(key []byte) []byte {
    // opcode, keyLength, extraLength, totalBodyLength
    header := binprot.MakeRequestHeader(binprot.OPCODE_GET, len(key), 0, len(key))
    
    reqBuf := new(bytes.Buffer)
    binary.Write(reqBuf, binary.BigEndian, header)
    
    binary.Write(reqBuf, binary.BigEndian, key)
    
    return reqBuf.Bytes()
    
	//return fmt.Sprintf("get %s\r\n", key)
}

func DeleteCommand(key []byte) []byte {
	return fmt.Sprintf("delete %s\r\n", key)
}

func TouchCommand(key []byte, exptime uint32) []byte {
	return fmt.Sprintf("touch %s %s\r\n", key, exptime)
}
