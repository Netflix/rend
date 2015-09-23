/**
 * Parses the text-based protocol and returns a command line
 * struct representing the work to be done
 */
package textprot

import "bufio"
import "errors"
import "fmt"
import "io"
import "strconv"
import "strings"

import "../common"

type TextParser struct {
    reader *bufio.Reader
}

func NewTextParser(reader io.Reader) TextParser {
    return TextParser {
        reader: bufio.NewReader(reader),
    }
}

func (t TextParser) Parse() (interface{}, common.RequestType, error) {
    
    data, err := t.reader.ReadString('\n')
    
    if err != nil {
        if err == io.EOF {
            fmt.Println("End of file: Connection closed?")
        }
        
        fmt.Println(err.Error())
        return nil, common.REQUEST_GET, err
    }
    
    clParts := strings.Split(strings.TrimSpace(data), " ")
    
    switch clParts[0] {
        case "set":
            // sanity check
            if len(clParts) != 5 {
                // TODO: standardize errors
                return nil, common.REQUEST_SET, errors.New("Bad request")
            }
            
            key := []byte(clParts[1])
            
            flags, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
            
            if err != nil {
                fmt.Println(err.Error())
                // TODO: standardize errors
                return nil, common.REQUEST_SET, common.BAD_FLAGS
            }
            
            exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[3]), 10, 32)
            
            if err != nil {
                fmt.Println(err.Error())
                // TODO: standardize errors
                return nil, common.REQUEST_SET, errors.New("Bad exptime")
            }
            
            length, err := strconv.ParseUint(strings.TrimSpace(clParts[4]), 10, 32)
            
            if err != nil {
                fmt.Println(err.Error())
                // TODO: standardize errors
                return nil, common.REQUEST_SET, common.BAD_LENGTH
            }
            
            return common.SetRequest {
                Key:     key,
                Flags:   uint32(flags),
                Exptime: uint32(exptime),
                Length:  uint32(length),
                Opaque:  uint32(0),
            }, common.REQUEST_SET, nil
            
        case "get":
            if len(clParts) < 2 {
                // TODO: standardize errors
                return nil, common.REQUEST_GET, errors.New("Bad get")
            }

            keys := make([][]byte, 0)
            for _, key := range clParts[1:] {
                keys = append(keys, []byte(key))
            }

            opaques := make([]uint32, len(keys))

            return common.GetRequest {
                Keys:    keys,
                Opaques: opaques,
            }, common.REQUEST_GET, nil
            
        case "delete":
            if len(clParts) != 2 {
                // TODO: standardize errors
                return nil, common.REQUEST_DELETE, errors.New("Bad delete")
            }
            
            return common.DeleteRequest {
                Key:    []byte(clParts[1]),
                Opaque: uint32(0),
            }, common.REQUEST_DELETE, nil
            
        // TODO: Error handling for invalid cmd line
        case "touch":
            if len(clParts) != 3 {
                // TODO: standardize errors
                return nil, common.REQUEST_TOUCH, errors.New("Bad touch")
            }
            
            key := []byte(clParts[1])
        
            exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
            
            if err != nil {
                fmt.Println(err.Error())
                // TODO: standardize errors
                return nil, common.REQUEST_SET, errors.New("Bad exptime")
            }
            
            return common.TouchRequest {
                Key:     key,
                Exptime: uint32(exptime),
                Opaque:  uint32(0),
            }, common.REQUEST_TOUCH, nil
    }
    
    return nil, common.REQUEST_UNKNOWN, nil
}
