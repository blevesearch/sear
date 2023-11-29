//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package sear

import (
	"bytes"
	"context"

	index "github.com/blevesearch/bleve_index_api"
)

func (d *Document) interpretVectorIfApplicable(field index.Field) int {
	if vf, ok := field.(index.VectorField); ok {
		return vf.Dims()
	}

	return 0
}

func (r *Reader) VectorIndexReader(ctx context.Context, vector []float32,
	field string, k int64) (index.VectorReader, error) {
	if r.s.doc == nil {
		return NewVectorFieldReaderEmpty(), nil
	}

	dims, err := r.s.doc.VectorDims(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return NewVectorFieldReaderEmpty(), nil
	}

	if dims != len(vector) {
		// no match
		return NewVectorFieldReaderEmpty(), nil
	}

	return NewVectorFieldReaderMatch(dims), nil

}

// -----------------------------------------------------------------------------

type VectorFieldReader struct {
	done bool
	dims int
}

func NewVectorFieldReaderEmpty() *VectorFieldReader {
	return &VectorFieldReader{
		done: true,
	}
}

func NewVectorFieldReaderMatch(dims int) *VectorFieldReader {
	return &VectorFieldReader{
		dims: dims,
	}
}

func (v *VectorFieldReader) Next(preAlloced *index.VectorDoc) (*index.VectorDoc, error) {
	if v.done {
		return nil, nil
	}
	rv := preAlloced
	if rv == nil {
		rv = &index.VectorDoc{}
	}
	rv.ID = internalDocID
	v.done = true
	return rv, nil
}

func (v *VectorFieldReader) Advance(id index.IndexInternalID, preAlloced *index.VectorDoc) (*index.VectorDoc, error) {
	if v.done {
		return nil, nil
	}
	if bytes.Compare(id, internalDocID) > 0 {
		// seek is after our internal id
		v.done = true
		return nil, nil
	}
	return v.Next(preAlloced)
}

func (v *VectorFieldReader) Count() uint64 {
	if v.dims != 0 {
		return 1
	}
	return 0
}

func (v *VectorFieldReader) Close() error {
	return nil
}

func (v *VectorFieldReader) Size() int {
	return 0
}
