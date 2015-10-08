package binprot

import "encoding/binary"
import "io"
import "math/rand"
import "time"

import "../common"

const MAX_TTL = 3600
var garbage []byte

func init() {
    rand.Seed(time.Now().UnixNano())
    garbage = make([]byte, 4096)
}

type BinProt struct {}

func consumeResponse(reader io.Reader, n uint32) error {
    lr := io.LimitReader(reader, int64(n))
    var err error
    for err == nil {
        _, err = lr.Read(garbage);
    }
    
    if err == io.EOF {
        return nil
    }
    return err
}

func (b BinProt) Set(reader io.Reader, writer io.Writer, key, value []byte) error {
    // set packet contains the req header, flags, and expiration
    // flags are irrelevant, and are thus zero.
    // expiration could be important, so hammer with random values from 1 sec up to 1 hour
    bodylen := 8 + len(key) + len(value)
    writeReq(writer, common.SET, len(key), 8, bodylen)
    binary.Write(writer, binary.BigEndian, uint32(0))
    binary.Write(writer, binary.BigEndian, uint32(rand.Intn(MAX_TTL)))
    writer.Write(key)
    writer.Write(value)

    res, err := readRes(reader)
    if err != nil {
        return err
    }

    err = statusToError(res.Status)
    if err != nil {
        return err
    }

    // consume all of the response and discard
    return consumeResponse(reader, res.BodyLen)
}

func (b BinProt) Get(reader io.Reader, writer io.Writer, key []byte) error {/*
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
    return nil*/ return nil
}

func (b BinProt) Delete(reader io.Reader, writer io.Writer, key []byte) error {/*
    strKey := string(key)
    if VERBOSE { fmt.Printf("Deleting key %s\r\n", strKey) }
    
    _, err := fmt.Fprintf(writer, "delete %s\r\n", strKey)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Deleted key %s\r\n", strKey) }
    return nil*/ return nil
}

func (b BinProt) Touch(reader io.Reader, writer io.Writer, key []byte) error {/*
    strKey := string(key)
    if VERBOSE { fmt.Printf("Touching key %s\r\n", strKey) }
    
    _, err := fmt.Fprintf(writer, "touch %s 123456\r\n", strKey)
    if err != nil { return err }
    writer.Flush()
    
    response, err := reader.ReadString('\n')
    if err != nil { return err }
    if VERBOSE { fmt.Println(response) }
    
    if VERBOSE { fmt.Printf("Touched key %s\r\n", strKey) }
    return nil*/ return nil
}
