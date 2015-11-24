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
 * Functions that talk to the local memcached server
 */
package local

import "bufio"
import "encoding/binary"
import "io"

import "../binprot"
import "../common"

func getMetadata(localReader *bufio.Reader, localWriter *bufio.Writer, key []byte) ([]byte, common.Metadata, error) {
    metaKey := metaKey(key)

    // Read in the metadata for number of chunks, chunk size, etc.
    getCmd := binprot.GetCmd(metaKey)
    
    _, err := localWriter.Write(getCmd)
    if err != nil {
        return nil, common.Metadata{}, err
    }
    
    localWriter.Flush()
    
    resHeader, err := binprot.ReadResponseHeader(localReader)
    if err != nil {
        return nil, common.Metadata{}, err
    }
    
    err = binprot.DecodeError(resHeader)
    if err != nil {
        if err == common.ERROR_KEY_NOT_FOUND {
            // read in the message "Not found" after a miss 
            garbage := make([]byte, resHeader.TotalBodyLength)
            _, ioerr := io.ReadFull(localReader, garbage)

            if ioerr != nil {
                return nil, common.Metadata{}, ioerr
            }
        }

        return nil, common.Metadata{}, err
    }

    serverFlags := make([]byte, 4)
    binary.Read(localReader, binary.BigEndian, &serverFlags)

    var metaData common.Metadata
    binary.Read(localReader, binary.BigEndian, &metaData)

    return metaKey, metaData, nil
}

// TODO: stream data through instead of buffering the entire value
func setLocal(localWriter *bufio.Writer, cmd []byte, token *[16]byte, data io.Reader) error {
    _, err := localWriter.Write(cmd)
    if err != nil {
        return err
    }

    // Write a token if there is one
    if token != nil {
        _, err = localWriter.Write(token[:])
        if err != nil {
            return err
        }
    }

    // Write value
    _, err = io.Copy(localWriter, data)
    if err != nil {
        return err
    }

    localWriter.Flush()
    return nil
}

func simpleCmdLocal(localReader *bufio.Reader, localWriter *bufio.Writer, cmd []byte) error {
    _, err := localWriter.Write(cmd)
    if err != nil {
        return err
    }
    
    err = localWriter.Flush()
    if err != nil {
        return err
    }

    resHeader, err := binprot.ReadResponseHeader(localReader)
    if err != nil {
        return err
    }
    
    err = binprot.DecodeError(resHeader)
    if err != nil {
        return err
    }

    // Read in the message bytes from the body
    localReader.Discard(int(resHeader.TotalBodyLength))
    if err != nil {
        return err
    }
    
    return nil
}

// TODO: Batch get
func getLocalIntoBuf(localReader *bufio.Reader, localWriter *bufio.Writer,
                     cmd []byte, tokenBuf []byte, buf []byte, totalDataLength int) error {
    _, err := localWriter.Write(cmd)
    if err != nil {
        return err
    }
    
    localWriter.Flush()

    resHeader, err := binprot.ReadResponseHeader(localReader)
    if err != nil {
        return err
    }
    
    err = binprot.DecodeError(resHeader)
    if err != nil {
        return err
    }
    
    serverFlags := make([]byte, 4)
    binary.Read(localReader, binary.BigEndian, &serverFlags)

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

    // consume padding at end of chunk if needed
    if len(buf) < totalDataLength {
        garbage := make([]byte, totalDataLength - len(buf))
        _, err = io.ReadFull(localReader, garbage)

        if err != nil {
            return err
        }
    }

    return nil
}
