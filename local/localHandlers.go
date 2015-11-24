/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Local request handlers that perform higher level logic.
 */
package local

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"
import "math"

import "../binprot"
import "../common"
import "../stream"

// Chunk size, leaving room for the token
// Make sure the value subtracted from chunk size stays in sync
// with the size of the Metadata struct
const CHUNK_SIZE = 1024 - 16
const FULL_DATA_SIZE = 1024

func consumeResponseHeader(localReader *bufio.Reader) error {
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
    //buf := make([]byte, cmd.Length)
    //_, err := io.ReadFull(remoteReader, buf)

    // For writing chunks, the specialized chunked reader is appropriate.
    // for unchunked, a limited reader will be needed since the text protocol
    // includes a /r/n at the end and there's no EOF to be had with a long-lived
    // connection.
    limRemoteReader := stream.ChunkLimitReader(remoteReader, int64(CHUNK_SIZE), int64(cmd.Length))
    numChunks := int(math.Ceil(float64(cmd.Length) / float64(CHUNK_SIZE)))
    token := <-tokens
    
    metaKey := metaKey(cmd.Key)
    metaData := common.Metadata {
        Length:    cmd.Length,
        OrigFlags: cmd.Flags,
        NumChunks: uint32(numChunks),
        ChunkSize: CHUNK_SIZE,
        Token:     *token,
    }
    
    metaDataBuf := new(bytes.Buffer)
    binary.Write(metaDataBuf, binary.BigEndian, metaData)
    
    // Write metadata key
    // TODO: should there be a unique flags value for chunked data?
    localCmd := binprot.SetCmd(metaKey, cmd.Flags, cmd.Exptime, common.METADATA_SIZE)
    err := setLocal(localWriter, localCmd, nil, metaDataBuf)
    if err != nil {
        return err
    }
    
    // Read server's response
    err = consumeResponseHeader(localReader)
    if err != nil {
        return err
    } 
    
    // Write all the data chunks
    // TODO: Clean up if a data chunk write fails
    // Failure can mean the write failing at the I/O level
    // or at the memcached level, e.g. response == ERROR
    chunkNum := 0
    for limRemoteReader.More() {
        // Build this chunk's key
        key := chunkKey(cmd.Key, chunkNum)
        
        // Write the key
        localCmd = binprot.SetCmd(key, cmd.Flags, cmd.Exptime, FULL_DATA_SIZE)
        err = setLocal(localWriter, localCmd, token, limRemoteReader)
        if err != nil {
            return err
        }
        
        // Read server's response
        err := consumeResponseHeader(localReader)
        if err != nil {
            return err
        }

        // Reset for next iteration
        limRemoteReader.NextChunk()
        chunkNum++
    }
    
    return nil
}

func HandleGet(cmd common.GetRequest, localReader *bufio.Reader, localWriter *bufio.Writer) (chan common.GetResponse, chan error) {
    // No buffering here so there's not multiple gets in memory
    dataOut := make(chan common.GetResponse)
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

    defer close(errorOut)
    defer close(dataOut)
        
    outer: for _, key := range cmd.Keys {
        _, metaData, err := getMetadata(localReader, localWriter, key)
        if err != nil {
            // TODO: Better error management
            if err == common.MISS || err == common.ERROR_KEY_NOT_FOUND {
                fmt.Println("Get miss because of missing metadata. Key:", key)
                dataOut <- common.GetResponse {
                    Miss: true,
                    Key:  key,
                    //opaque
                }
                continue outer
            }
            
            errorOut <- err
            return
        }
        
        // Retrieve all the data from memcached
        dataBuf := make([]byte, metaData.Length)
        tokenBuf := make([]byte, 16)
        
        for i := 0; i < int(metaData.NumChunks); i++ {
            chunkKey := chunkKey(key, i)
            
            // indices for slicing, end exclusive
            start, end := chunkSliceIndices(int(metaData.ChunkSize), i, int(metaData.Length))
            
            // Get the data directly into our buf
            chunkBuf := dataBuf[start:end]
            getCmd := binprot.GetCmd(chunkKey)
            err = getLocalIntoBuf(localReader, localWriter, getCmd, tokenBuf, chunkBuf, int(metaData.ChunkSize))
            
            if err != nil {
                // TODO: Better error management
                if err == common.MISS || err == common.ERROR_KEY_NOT_FOUND {
                    fmt.Println("Get miss because of missing chunk. Cmd:", getCmd)
                    dataOut <- common.GetResponse {
                        Miss: true,
                        Key:  key,
                        //opaque
                    }
                    continue outer
                }
                
                errorOut <- err
                return
            }
            
            if (!bytes.Equal(metaData.Token[:], tokenBuf)) {
                fmt.Println("Get miss because of invalid chunk token. Cmd:", getCmd)
                fmt.Printf("Expected: %v\n", metaData.Token)
                fmt.Printf("Got:      %v\n", tokenBuf)
                dataOut <- common.GetResponse {
                    Miss: true,
                    Key:  key,
                    //opaque
                }
                continue outer
            }
        }
        
        dataOut <- common.GetResponse {
            Miss:     false,
            Key:      key,
            // opaque
            Metadata: metaData,
            Data:     dataBuf,
        }
    }
}

func HandleDelete(cmd common.DeleteRequest, localReader *bufio.Reader, localWriter *bufio.Writer) error {
    // read metadata
    // delete metadata
    // for 0 to metadata.numChunks
    //  delete item
    
    metaKey, metaData, err := getMetadata(localReader, localWriter, cmd.Key)
    
    if err != nil {
        if err == common.MISS {
            fmt.Println("Delete miss because of missing metadata. Key:", cmd.Key)
        }
        return err
    }
    
    deleteCmd := binprot.DeleteCmd(metaKey)
    err = simpleCmdLocal(localReader, localWriter, deleteCmd)
    if err != nil {
        return err
    }
    
    for i := 0; i < int(metaData.NumChunks); i++ {
        chunkKey := chunkKey(cmd.Key, i)
        deleteCmd = binprot.DeleteCmd(chunkKey)
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
        if err == common.MISS {
            fmt.Println("Touch miss because of missing metadata. Key:", cmd.Key)
            return err
        }
        
        return err
    }
    
    for i := 0; i < int(metaData.NumChunks); i++ {
        chunkKey := chunkKey(cmd.Key, i)
        touchCmd := binprot.TouchCmd(chunkKey, cmd.Exptime)
        err := simpleCmdLocal(localReader, localWriter, touchCmd)
        if err != nil {
            return err
        }
    }
    
    touchCmd := binprot.TouchCmd(metaKey, cmd.Exptime)
    err = simpleCmdLocal(localReader, localWriter, touchCmd)
    if err != nil {
        return err
    }
    
    return nil
}
