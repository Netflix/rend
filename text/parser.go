/**
 * Parses the text-based protocol and returns a command line
 * struct representing the work to be done
 */
package text

import "bufio"
import "bytes"
import "crypto/rand"
import "encoding/binary"
import "errors"
import "fmt"
import "io"
import "math"
import "net"
import "strconv"
import "strings"

import "../common"

func Parse(remoteReader *bufio.Reader) (interface{}, error) {
    
    data, err := remoteReader.ReadString('\n')
    
    if err != nil {
        if err == io.EOF {
            fmt.Println("End of file: Connection closed?")
        } else {
            fmt.Println(err.Error())
        }
        return interface{}, err
    }
    
    clParts := strings.Split(strings.TrimSpace(data), " ")
    
    switch clParts[0] {
        case "set":
            length, err := strconv.Atoi(strings.TrimSpace(clParts[4]))
            
            if err != nil {
                fmt.Println(err.Error())
                return interface{}, common.BAD_LENGTH
            }
            
            flags, err := strconv.Atoi(strings.TrimSpace(clParts[2]))
            
            if err != nil {
                fmt.Println(err.Error())
                return interface{}, common.BAD_FLAGS
            }
            
            return SetCmdLine {
                cmd:     clParts[0],
                key:     clParts[1],
                flags:   flags,
                exptime: clParts[3],
                length:  length,
            }, nil
            
        case "get":
            return GetCmdLine {
                cmd:  clParts[0],
                keys: clParts[1:],
            }, nil
            
        case "delete":
            return DeleteCmdLine {
                cmd: clParts[0],
                key: clParts[1],
            }, nil
            
        // TODO: Error handling for invalid cmd line
        case "touch":
            return TouchCmdLine {
                cmd:     clParts[0],
                key:     clParts[1],
                exptime: clParts[2],
            }, nil
    }
}
