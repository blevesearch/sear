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
	"math"

	index "github.com/blevesearch/bleve_index_api"
)

type TermFieldReader struct {
	tf                 *index.TokenFreq
	len                int
	done               bool
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

var termFieldReaderEmpty = NewTermFieldReaderEmpty()

func NewTermFieldReaderEmpty() *TermFieldReader {
	return &TermFieldReader{
		done: true,
	}
}

func NewTermFieldReaderFromTokenFreqAndLen(tf *index.TokenFreq, l int, includeFreq, includeNorm,
	includeTermVectors bool) *TermFieldReader {
	return &TermFieldReader{
		tf:                 tf,
		len:                l,
		includeFreq:        includeFreq,
		includeNorm:        includeNorm,
		includeTermVectors: includeTermVectors,
	}
}

func normForLen(l int) float64 {
	return float64(float32(1 / math.Sqrt(float64(l))))
}

func (t *TermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if t.done {
		return nil, nil
	}
	rv := preAlloced
	if rv == nil {
		rv = &index.TermFieldDoc{}
	}
	rv.Term = string(t.tf.Term)
	rv.ID = internalDocID
	if t.includeFreq {
		rv.Freq = uint64(t.tf.Frequency())
	}
	if t.includeNorm {
		rv.Norm = normForLen(t.len)
	}
	if t.includeTermVectors {
		locs := t.tf.Locations
		if cap(rv.Vectors) < len(locs) {
			rv.Vectors = make([]*index.TermFieldVector, len(locs))
			backing := make([]index.TermFieldVector, len(locs))
			for i := range backing {
				rv.Vectors[i] = &backing[i]
			}
		}
		rv.Vectors = rv.Vectors[:len(locs)]
		for i, loc := range locs {
			*rv.Vectors[i] = index.TermFieldVector{
				Start:          uint64(loc.Start),
				End:            uint64(loc.End),
				Pos:            uint64(loc.Position),
				ArrayPositions: loc.ArrayPositions,
				Field:          loc.Field,
			}
		}
	}
	t.done = true
	return rv, nil
}

// Advance resets the enumeration at specified document or its immediate
// follower.
func (t *TermFieldReader) Advance(id index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if t.done {
		return nil, nil
	}
	if bytes.Compare(id, internalDocID) > 0 {
		// seek is after our internal id
		t.done = true
		return nil, nil
	}
	return t.Next(preAlloced)
}

func (t *TermFieldReader) Count() uint64 {
	if t.tf != nil {
		return 1
	}
	return 0
}

func (t *TermFieldReader) Close() error {
	return nil
}

func (t *TermFieldReader) Size() int {
	return 0
}
