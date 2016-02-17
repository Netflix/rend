package binprot_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
)

func TestUnknownCommand(t *testing.T) {
	r := bufio.NewReader(bytes.NewBuffer([]byte{
		0x80,       // Magic
		0xFF,       // Bad opcode
		0x00, 0x00, // key length
		0x00,       // Extra length
		0x00,       // Data type
		0x00, 0x00, // VBucket
		0x00, 0x00, 0x00, 0x04, // total body length
		0x00, 0x00, 0x00, 0xA5, // opaque token
		0x00, 0x00, 0x00, 0x00, // CAS
		0x00, 0x00, 0x00, 0x00, // CAS
	}))
	req, reqType, err := binprot.NewBinaryParser(r).Parse()

	if req != nil {
		t.Fatal("Expected request struct to be nil")
	}
	if reqType != common.RequestUnknown {
		t.Fatal("Expected request type to be Unknown")
	}
	if err != common.ErrUnknownCmd {
		t.Fatal("Expected error to be Unknown Command")
	}
}
