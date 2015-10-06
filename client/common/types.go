package common

import "bufio"

type Prot interface {
    Set   (reader *bufio.Reader, writer *bufio.Writer, key []byte, value []byte) error
    Get   (reader *bufio.Reader, writer *bufio.Writer, key []byte              ) error
    Delete(reader *bufio.Reader, writer *bufio.Writer, key []byte              ) error
    Touch (reader *bufio.Reader, writer *bufio.Writer, key []byte              ) error
}