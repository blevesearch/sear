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
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
)

const Name = "sear"

// Sear implements an index containing a single document.
type Sear struct {
	doc *Document

	internal map[string][]byte
	stats    map[string]interface{}
	reader   *Reader
}

// New creates a new instance of a Sear index.
// This method signature is compatible with the
// Bleve registry RegisterIndexType() method.
//
// For example, in your application init()
// registry.RegisterIndexType(search.Name, sear.New)
func New(storeName string,
	config map[string]interface{},
	analysisQueue *index.AnalysisQueue) (index.Index, error) {
	rv := &Sear{
		internal: make(map[string][]byte),
	}

	rv.reader = NewReader(rv)

	return rv, nil
}

// Open the index
func (s *Sear) Open() error {
	return nil
}

// Close the index
func (s *Sear) Close() error {
	return nil
}

// Update the index to include this document.
// Unlike other Bleve indexes, this operation will overwrite
// a previously indexed document, regardless of the document's
// identifiers.
func (s *Sear) Update(doc index.Document) error {
	if s.doc == nil {
		s.doc = NewDocument()
	}
	s.doc.Reset(doc)

	return nil
}

// Delete document from the index.
// Unlike other Bleve indexes, this operation will delete
// the document from the index, regardless of it's identifier.
func (s *Sear) Delete(id string) error {
	s.doc = nil
	return nil
}

// Batch is not supported by this index.
func (s *Sear) Batch(batch *index.Batch) error {
	return fmt.Errorf("batch indexing is not supported by this index")
}

// SetInternal sets a value in the index internal storage.
func (s *Sear) SetInternal(key, val []byte) error {
	s.internal[string(key)] = val
	return nil
}

// DeleteInternal deletes a value from the index internal storage.
func (s *Sear) DeleteInternal(key []byte) error {
	delete(s.internal, string(key))
	return nil
}

// Reader returns a reader for this index.
// Unlike other Bleve indexes, this reader is NOT isolated.
func (s *Sear) Reader() (index.IndexReader, error) {
	return s.reader, nil
}

// StatsMap returns stats about this index.
func (s *Sear) StatsMap() map[string]interface{} {
	return s.stats
}
