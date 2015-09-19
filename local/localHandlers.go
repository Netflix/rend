/**
 * Local request handlers that perform higher level logic.
 */
package local

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"
import "io"
import "math"

import "../binprot"
import "../common"
import "../util"

func consumeResponse(localReader *bufio.Reader) error {
    // Read server's response
    // TODO: Error handling of ERROR response
    resHeader, err := binprot.ReadResponseHeader(localReader)
    if err != nil {
        return err
    }
    
    err = binprot.DecodeError(resHeader)
    if err != nil {
        return err
    }
    
    return nil
}

func HandleSet(cmd common.SetRequest, remoteReader *bufio.Reader, localReader *bufio.Reader, localWriter *bufio.Writer) error {
    // Read in the data from the remote connection
    buf := make([]byte, cmd.Length)
    _, err := io.ReadFull(remoteReader, buf)
    // TODO: text protocol bytes?
    
    numChunks := int(math.Ceil(float64(cmd.Length) / float64(common.CHUNK_SIZE)))
    token := <-tokens
    
    metaKey := util.MetaKey(cmd.Key)
    metaData := common.Metadata {
        Length:    cmd.Length,
        OrigFlags: cmd.Flags,
        NumChunks: uint32(numChunks),
        ChunkSize: common.CHUNK_SIZE,
        Token:     *token,
    }
    
    metaDataBuf := new(bytes.Buffer)
    binary.Write(metaDataBuf, binary.BigEndian, metaData)
    
    // Write metadata key
    // TODO: should there be a unique flags value for chunked data?
    localCmd := util.SetCmd(metaKey, cmd.Flags, cmd.Exptime, common.METADATA_SIZE)
    err = common.SetLocal(localWriter, localCmd, nil, metaDataBuf.Bytes())
    if err != nil {
        return err
    }
    
    // Read server's response
    err = consumeResponse(localReader)
    if err != nil {
        return err
    } 
    
    // Write all the data chunks
    // TODO: Clean up if a data chunk write fails
    // Failure can mean the write failing at the I/O level
    // or at the memcached level, e.g. response == ERROR
    for i := 0; i < numChunks; i++ {
        // Build this chunk's key
        key := util.ChunkKey(cmd.Key, i)
        
        // indices for slicing, end exclusive
        start, end := util.SliceIndices(i, cmd.Length)
        chunkBuf := buf[start:end]
        
        // Pad the data to always be common.CHUNK_SIZE
        if (end-start) < common.CHUNK_SIZE {
            padding := common.CHUNK_SIZE - (end-start)
            padtext := bytes.Repeat([]byte{byte(0)}, padding)
            chunkBuf = append(chunkBuf, padtext...)
        }
        
        // Write the key
        localCmd = util.SetCommand(key, cmd.Flags, cmd.Exptime, FULL_DATA_SIZE)
        err = setLocal(localWriter, localCmd, token, chunkBuf)
        if err != nil {
            return err
        }
        
        // Read server's response
        err := consumeResponse(localReader)
        if err != nil {
            return err
        }
    }
    
    return nil
}

func HandleGet(cmd common.GetRequest, localReader *bufio.Reader, localWriter *bufio.Writer) (chan common.GetResponse, chan error) {
    // No buffering here so there's not multiple gets in memory
    dataOut := make(chan GetResponse)
    errorOut := make(chan error)
    go realHandleGet(cmd, dataOut, errorOut, localReader, localWriter)
    return dataOut, errorOut
}

func realHandleGet(cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error,
                   localReader *bufio.Reader, localWriter *bufio.Writer) {
    // read index
    // make buf
    // for numChunks do
    //   read chunk, append to buffer
    // send response
        
    outer: for _, key := range cmd.Keys {
        _, metaData, err := getMetadata(localReader, localWriter, key)
        if err != nil {
            if err == MISS {
                fmt.Println("Get miss because of missing metadata. Key:", key)
                continue outer
            }
            
            errorOut <- err
            close(errorOut)
            close(dataOut)
            return
        }
        
        // Retrieve all the data from memcached
        dataBuf := make([]byte, metaData.Length)
        tokenBuf := make([]byte, 16)
        
        for i := 0; i < int(metaData.NumChunks); i++ {
            chunkKey := makeChunkKey(key, i)
            
            // indices for slicing, end exclusive
            // TODO: pass chunk size
            start, end := sliceIndices(i, int(metaData.Length))
            
            // Get the data directly into our buf
            chunkBuf := dataBuf[start:end]
            getCmd := makeGetCommand(chunkKey)
            err = getLocalIntoBuf(localReader, localWriter, getCmd, tokenBuf, chunkBuf)
            
            if err != nil {
                if err == MISS {
                    fmt.Println("Get miss because of missing chunk. Cmd:", getCmd)
                    continue outer
                }
                
                errorOut <- err
                close(errorOut)
                close(dataOut)
                return
            }
            
            if (!bytes.Equal(metaData.Token[:], tokenBuf)) {
                fmt.Println("Get miss because of invalid chunk token. Cmd:", getCmd)
                continue outer
            }
        }
        
        dataOut <- GetResponse {
            Key:      key,
            Metadata: metaData,
            Data:     dataBuf,
        }
    }
    
    close(dataOut)
    close(errorOut)
}

// TODO: Batch get
func getLocalIntoBuf(localReader *bufio.Reader, localWriter *bufio.Writer, cmd string, tokenBuf []byte, buf []byte) error {
	_, err := localWriter.WriteString(cmd)
	if err != nil {
		return err
	}
    
	localWriter.Flush()

	// Read and discard value header line
	line, err := localReader.ReadString('\n')

	if strings.TrimSpace(line) == "END" {
		return common.MISS
	}

	// Read in token if requested
	if tokenBuf != nil {
		_, err := io.ReadFull(localReader, tokenBuf)
		if err != nil {
			return err
		}
	}

	// Read in value
    if _, err := io.ReadFull(localReader, buf); err != nil {
		return err
	}

	// consume END
	_, err = localReader.ReadBytes('\n')

	return nil
}

func HandleDelete(cmd common.DeleteRequest, localReader *bufio.Reader, localWriter *bufio.Writer) error {
    // read metadata
    // delete metadata
    // for 0 to metadata.numChunks
    //  delete item
    
    metaKey, metaData, err := getMetadata(localReader, localWriter, cmd.Key)
    
    if err != nil {
        if err == MISS {
            fmt.Println("Delete miss because of missing metadata. Key:", cmd.Key)
        }
        return err
    }
    
    deleteCmd := util.DeleteCommand(metaKey)
    err = simpleCmdLocal(localReader, localWriter, deleteCmd)
    if err != nil {
        return err
    }
    
    for i := 0; i < int(metaData.NumChunks); i++ {
        chunkKey := makeChunkKey(cmd.Key, i)
        deleteCmd = util.DeleteCommand(chunkKey)
        err := simpleCmdLocal(localReader, localWriter, deleteCmd)
        if err != nil {
            return err
        }
    }
    
    return nil
}

func HandleTouch(cmd common.TouchRequest, localReader *bufio.Reader, localWriter *bufio.Writer) error {
    // read metadata
    // for 0 to metadata.numChunks
    //  touch item
    // touch metadata
    
    metaKey, metaData, err := getMetadata(localReader, localWriter, cmd.Key)
        
    if err != nil {
        if err == MISS {
            fmt.Println("Touch miss because of missing metadata. Key:", cmd.Key) 
            return err
        }
        
        return err
    }
    
    for i := 0; i < int(metaData.NumChunks); i++ {
        chunkKey := makeChunkKey(cmd.Key, i)
        touchCmd := util.TouchCommand(chunkKey, cmd.Exptime)
        err := simpleCmdLocal(localReader, localWriter, touchCmd)
        if err != nil {
            return err
        }
    }
    
    touchCmd = util.TouchCommand(metaKey, exptime)
    err = simpleCmdLocal(localReader, localWriter, touchCmd)
    if err != nil {
        return err
    }
    
    return nil
}
