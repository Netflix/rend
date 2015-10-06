package textprot

import "bufio"
import "fmt"
import "strings"

const VERBOSE = false

type TextProt struct {}

func (t TextProt) Set(reader *bufio.Reader, writer *bufio.Writer, key []byte, value []byte) error {
    strKey := string(key)
    if VERBOSE { fmt.Printf("Setting key %v to value of length %v\r\n", strKey, len(value)) }

    _, err := fmt.Fprintf(writer, "set %v 0 0 %v\r\n", strKey, len(value))
    if err != nil { return err }
    _, err = fmt.Fprintf(writer, "%v\r\n", string(value))
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }

    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Set key %v\r\n", strKey) }
    return nil
}

func (t TextProt) Get(reader *bufio.Reader, writer *bufio.Writer, key []byte) error {
    strKey := string(key)
    if VERBOSE { fmt.Printf("Getting key %v\r\n", strKey) }

    _, err := fmt.Fprintf(writer, "get %v\r\n", strKey)
    if err != nil { return err }
    writer.Flush()
    
    // read the header line
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if strings.TrimSpace(response) == "END" {
        if VERBOSE { fmt.Println("Empty response / cache miss") }
        return nil
    }

    // then read the value
    response, err = reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }

    // then read the END
    response, err = reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Got key %v\r\n", key) }
    return nil
}

func (t TextProt) Delete(reader *bufio.Reader, writer *bufio.Writer, key []byte) error {
    strKey := string(key)
    if VERBOSE { fmt.Printf("Deleting key %s\r\n", strKey) }
    
    _, err := fmt.Fprintf(writer, "delete %s\r\n", strKey)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Deleted key %s\r\n", strKey) }
    return nil
}

func (t TextProt) Touch(reader *bufio.Reader, writer *bufio.Writer, key []byte) error {
    strKey := string(key)
    if VERBOSE { fmt.Printf("Touching key %s\r\n", strKey) }
    
    _, err := fmt.Fprintf(writer, "touch %s 123456\r\n", strKey)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Touched key %s\r\n", strKey) }
    return nil
}
