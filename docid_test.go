//  Copyright (c) 2021 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//              http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sear

import (
	"bytes"
	"io"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func TestDocIDReader(t *testing.T) {
	// first test empty
	var docIDReader *DocIDReader = docIDReaderEmpty
	_, err := docIDReader.Next()
	if err != io.EOF {
		t.Fatalf("expected eof")
	}

	_, err = docIDReader.Advance(internalDocID)
	if err != io.EOF {
		t.Fatalf("expected eof")
	}

	// now test non-empty with no advance
	docIDReader = NewDocIDReader()
	var internal index.IndexInternalID
	internal, err = docIDReader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(internal, internalDocID) {
		t.Fatalf("expected %v, got %v", internalDocID, internal)
	}

	_, err = docIDReader.Next()
	if err != io.EOF {
		t.Errorf("expected eof")
	}

	// test empty again with advance
	docIDReader = NewDocIDReader()
	internal, err = docIDReader.Advance(internalDocID)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if !bytes.Equal(internal, internalDocID) {
		t.Fatalf("expected %v, got %v", internalDocID, internal)
	}

	_, err = docIDReader.Next()
	if err != io.EOF {
		t.Errorf("expected eof")
	}

	// test empty again with advance to other internal id
	docIDReader = NewDocIDReader()
	_, err = docIDReader.Advance([]byte{0x1})
	if err != io.EOF {
		t.Errorf("expected eof")
	}

	err = docIDReader.Close()
	if err != nil {
		t.Errorf("error closing doc id reader: %v", err)
	}
}
