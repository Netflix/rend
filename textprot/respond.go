/**
 * Functions to respond in kind to text-based requests
 */
package textprot

import "bufio"
import "fmt"

import "../common"

type TextResponder struct {
    writer *bufio.Writer
}

func NewTextResponder(writer *bufio.Writer) TextResponder {
    return TextResponder {
        writer: writer,
    }
}

func (t TextResponder) Set() error {
    // TODO: Error handling for less bytes
    //numWritten, err := writer.WriteString("STORED\r\n")
    _, err := t.writer.WriteString("STORED\r\n")
    if err != nil { return err }
    
    err = t.writer.Flush()
    if err != nil { return err }
    
    return nil
}

func (t TextResponder) Get(response common.GetResponse) error {
    // Write data out to client
    // [VALUE <key> <flags> <bytes>\r\n
    // <data block>\r\n]*
    // END\r\n
    _, err := fmt.Fprintf(t.writer, "VALUE %s %d %d\r\n",
        response.Key, response.Metadata.OrigFlags, response.Metadata.Length)
    if err != nil { return err }
    
    _, err = t.writer.Write(response.Data)
    if err != nil { return err }
    
    _, err = t.writer.WriteString("\r\n")
    if err != nil { return err }

    t.writer.Flush()
    return nil
}

func (t TextResponder) GetMiss(response common.GetResponse) error {
    // A miss is a no-op in the text world
    return nil
}

func (t TextResponder) GetEnd() error {
    _, err := fmt.Fprintf(t.writer, "END\r\n")
    if err != nil { return err }
    
    t.writer.Flush()
    return nil
}

func (t TextResponder) Delete() error {
    _, err := fmt.Fprintf(t.writer, "DELETED\r\n")
    if err != nil { return err }
    
    t.writer.Flush()
    return nil
}

func (t TextResponder) Touch() error {
    _, err := fmt.Fprintf(t.writer, "TOUCHED\r\n")
    if err != nil { return err }
    
    t.writer.Flush()
    return nil
}

func (t TextResponder) Error(err error) error {
    if err == common.MISS {
        _, err = fmt.Fprintf(t.writer, "NOT_FOUND\r\n")
    } else {
        fmt.Println(err.Error())
        _, err = fmt.Fprintf(t.writer, "ERROR\r\n")
    }
    
    if err != nil { return err }
    t.writer.Flush()
    return nil
}

