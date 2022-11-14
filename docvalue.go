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
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
)

type DocValueReader struct {
	r      *Reader
	fields []string
}

func (d *DocValueReader) VisitDocValues(id index.IndexInternalID, visitor index.DocValueVisitor) error {
	if d.r.s.doc == nil {
		return nil
	}
	if !bytes.Equal(id, internalDocID) {
		return fmt.Errorf("unknown doc id: '%v", id)
	}

	for _, dvrField := range d.fields {
		atf, _, err := d.r.s.doc.TokenFreqsAndLen(dvrField)
		if err == nil {
			for _, v := range atf {
				visitor(dvrField, v.Term)
			}
		}
	}
	return nil
}

func (d *DocValueReader) BytesRead() uint64 {
	// not implemented
	return 0
}
