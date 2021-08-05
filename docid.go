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

	index "github.com/blevesearch/bleve_index_api"
)

type DocIDReader struct {
	done bool
}

var docIDReaderEmpty = NewDocIDReaderEmpty()

func NewDocIDReaderEmpty() *DocIDReader {
	return &DocIDReader{
		done: true,
	}
}

func NewDocIDReader() *DocIDReader {
	return &DocIDReader{}
}

func (d *DocIDReader) Next() (index.IndexInternalID, error) {
	if d.done {
		return nil, nil
	}
	d.done = true
	return internalDocID, nil
}

func (d *DocIDReader) Advance(id index.IndexInternalID) (index.IndexInternalID, error) {
	if d.done {
		return nil, nil
	}
	if bytes.Compare(id, internalDocID) > 0 {
		// seek is after our internal id
		d.done = true
		return nil, nil
	}
	return d.Next()
}

func (d *DocIDReader) Size() int {
	return 0
}

func (d *DocIDReader) Close() error {
	return nil
}
