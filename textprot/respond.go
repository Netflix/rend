/**
 * Functions to respond in kind to text-based requests
 */
package textprot

import "bufio"
import "fmt"

import "../common"

func RespondSet(err error, remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    // BAD_LENGTH
    //BAD_FLAGS
    
    // TODO: Error handling for less bytes
    //numWritten, err := writer.WriteString("STORED\r\n")
    _, err = remoteWriter.WriteString("STORED\r\n")
    if err != nil { return err }
    
    err = remoteWriter.Flush()
    if err != nil { return err }
    
    return nil
}

func RespondGetChunk(response common.GetResponse, remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    // Write data out to client
    // [VALUE <key> <flags> <bytes>\r\n
    // <data block>\r\n]*
    // END\r\n
    _, err := fmt.Fprintf(remoteWriter, "VALUE %s %d %d\r\n",
        response.Key, response.Metadata.OrigFlags, response.Metadata.Length)
    if err != nil { return err }
    
    _, err = remoteWriter.Write(response.Data)
    if err != nil { return err }
    
    _, err = remoteWriter.WriteString("\r\n")
    if err != nil { return err }

    remoteWriter.Flush()
    return nil
}

func RespondGetEnd(remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    _, err := fmt.Fprintf(remoteWriter, "END\r\n")
    if err != nil { return err }
    
    remoteWriter.Flush()
    return nil
}

func RespondDelete(err error, remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    if err != nil {
        return respondError(err, remoteReader, remoteWriter)
    }
    
    _, err = fmt.Fprintf(remoteWriter, "DELETED\r\n")
    if err != nil { return err }
    
    remoteWriter.Flush()
    return nil
}

func RespondTouch(err error, remoteReader *bufio.Reader, remoteWriter *bufio.Writer) error {
    if err != nil {
        return respondError(err, remoteReader, remoteWriter)
    }
    
    _, err = fmt.Fprintf(remoteWriter, "TOUCHED\r\n")
    if err != nil { return err }
    
    remoteWriter.Flush()
    return nil
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

