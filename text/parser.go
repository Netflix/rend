/**
 * Parses the text-based protocol and returns a command line
 * struct representing the work to be done
 */
package text

import "bufio"
import "fmt"
import "io"
import "strconv"
import "strings"

import "../common"

func ParseRequest(remoteReader *bufio.Reader) (interface{}, error) {
    
    data, err := remoteReader.ReadString('\n')
    
    if err != nil {
        if err == io.EOF {
            fmt.Println("End of file: Connection closed?")
        } else {
            fmt.Println(err.Error())
        }
        return nil, err
    }
    
    clParts := strings.Split(strings.TrimSpace(data), " ")
    
    switch clParts[0] {
        case "set":
            length, err := strconv.Atoi(strings.TrimSpace(clParts[4]))
            
            if err != nil {
                fmt.Println(err.Error())
                return nil, common.BAD_LENGTH
            }
            
            flags, err := strconv.Atoi(strings.TrimSpace(clParts[2]))
            
            if err != nil {
                fmt.Println(err.Error())
                return nil, common.BAD_FLAGS
            }
            
            return common.SetRequest {
                Cmd:     clParts[0],
                Key:     clParts[1],
                Flags:   flags,
                Exptime: clParts[3],
                Length:  length,
            }, nil
            
        case "get":
            return common.GetRequest {
                Cmd:  clParts[0],
                Keys: clParts[1:],
            }, nil
            
        case "delete":
            return common.DeleteRequest {
                Cmd: clParts[0],
                Key: clParts[1],
            }, nil
            
        // TODO: Error handling for invalid cmd line
        case "touch":
            return common.TouchRequest {
                Cmd:     clParts[0],
                Key:     clParts[1],
                Exptime: clParts[2],
            }, nil
    }
    
    return nil, nil
}
