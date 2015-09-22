/**
 * Memproxy is a proxy for memcached that will split the data input
 * into fixed-size chunks for storage. It will reassemble the data
 * on retrieval with set.
 */
package main

import "bufio"
import "fmt"
import "net"

import "./binprot"
import "./common"
import "./local"
import "./textprot"

const verbose = false

func main() {
    server, err := net.Listen("tcp", ":11212")
    
    if err != nil { print(err.Error()) }
    
    for {
        remote, err := server.Accept()
        
        if err != nil {
            fmt.Println(err.Error())
            remote.Close()
            continue
        }
        
        local, err := net.Dial("tcp", ":11211")
        
        if err != nil {
            fmt.Println(err.Error())
            if local != nil { local.Close() }
            remote.Close()
            continue
        }
        
        go handleConnection(remote, local)
    }
}

func abort(remote net.Conn, err error, binary bool) {
    fmt.Println("Error while processing request. Closing connection. Error:", err.Error())
    // use proper serializer to respond here
    remote.Close()
}

func handleConnection(remoteConn net.Conn, localConn net.Conn) {
    remoteReader := bufio.NewReader(remoteConn)
    remoteWriter := bufio.NewWriter(remoteConn)
    localReader  := bufio.NewReader(localConn)
    localWriter  := bufio.NewWriter(localConn)
    
    var parser    common.RequestParser
    var responder common.Responder
    var reqType   common.RequestType
    var request   interface{}
    
    var binaryParser    binprot.BinaryParser
    var binaryResponder binprot.BinaryResponder
    var textParser      textprot.TextParser
    var textResponder   textprot.TextResponder
    
    for {
        binary, err := isBinaryRequest(remoteReader)
        
        if err != nil {
            abort(remoteConn, err, binary)
            return
        }
        
        if binary {
            parser = binaryParser
            responder = binaryResponder
        } else {
            parser = textParser
            responder = textResponder
        }
        
        fmt.Println("About to parse request")
        request, reqType, err = parser.ParseRequest(remoteReader)
        fmt.Println("Parsed reqType: ", reqType)
        fmt.Println("Parsed request: ", request)
        
        if err != nil {
            abort(remoteConn, err, binary)
            return
        }
        
        // TODO: handle nil
        switch reqType {
            case common.REQUEST_SET:
                err = local.HandleSet(request.(common.SetRequest), remoteReader, localReader, localWriter)
                
                if err == nil {
                    // For text protocol, read in \r\n at end of data.
                    // A little hacky, but oh well.
                    if !binary {
                        _, err = remoteReader.ReadString('\n')
                    }

                    if err == nil {
                        responder.RespondSet(nil, remoteWriter)
                    }
                }

                
            case common.REQUEST_DELETE:
                err = local.HandleDelete(request.(common.DeleteRequest), localReader, localWriter)
                
                if err == nil {
                    responder.RespondDelete(nil, remoteWriter)
                }
                
            case common.REQUEST_TOUCH:
                err = local.HandleTouch(request.(common.TouchRequest), localReader, localWriter)
                
                if err == nil {
                    responder.RespondTouch(nil, remoteWriter)
                }
                
            case common.REQUEST_GET:
                resChan, errChan := local.HandleGet(request.(common.GetRequest), localReader, localWriter)
                
                for {
                    select {
                    case res, ok := <-resChan:
                        if !ok {
                            resChan = nil
                        } else {
                            fmt.Println("got chunk response: ", res)
                            responder.RespondGetChunk(res, remoteWriter)
                        }
                        
                    case getErr, ok := <-errChan:
                        if !ok {
                            errChan = nil
                        } else {
                            err = getErr
                        }
                    }
                    
                    if resChan == nil && errChan == nil {
                        break
                    }
                }
                
                responder.RespondGetEnd(remoteReader, remoteWriter)
        }
        
        // TODO: distinguish fatal errors from non-fatal
        if err != nil {
            abort(remoteConn, err, binary)
            return
        }
    }
}

func isBinaryRequest(reader *bufio.Reader) (bool, error) {
    headerByte, err := reader.Peek(1)
    if err != nil { return false, err }
    return int(headerByte[0]) == binprot.MAGIC_REQUEST, nil
}
