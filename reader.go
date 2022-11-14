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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/blevesearch/vellum"

	index "github.com/blevesearch/bleve_index_api"
	velreg "github.com/blevesearch/vellum/regexp"
)

var internalDocID = []byte{0}

// Reader is responsible for reading the index data
// It is also responsible for caching some portions
// of a read operation which can be used for subsequent
// reads.
type Reader struct {
	s *Sear

	velregCache map[string]*velreg.Regexp
	levSlice    []int
}

// NewReader returns a new reader for the provided Sear instance.
func NewReader(m *Sear) *Reader {
	rv := &Reader{
		s:           m,
		velregCache: make(map[string]*velreg.Regexp),
		levSlice:    make([]int, 64),
	}

	return rv
}

func (r *Reader) TermFieldReader(ctx context.Context, term []byte, field string, includeFreq, includeNorm,
	includeTermVectors bool) (index.TermFieldReader, error) {
	if r.s.doc == nil {
		return termFieldReaderEmpty, nil
	}
	atf, l, err := r.s.doc.TokenFreqsAndLen(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return termFieldReaderEmpty, nil
	}
	tf, ok := atf[string(term)]
	if !ok {
		return termFieldReaderEmpty, nil
	}

	return NewTermFieldReaderFromTokenFreqAndLen(tf, l, includeFreq, includeNorm, includeTermVectors), nil
}

func (r *Reader) DocIDReaderAll() (index.DocIDReader, error) {
	if r.s.doc == nil {
		return docIDReaderEmpty, nil
	}
	return NewDocIDReader(), nil
}

func (r *Reader) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	if r.s.doc == nil {
		return docIDReaderEmpty, nil
	}
	for _, id := range ids {
		if id == r.s.doc.doc.ID() {
			return NewDocIDReader(), nil
		}
	}
	return docIDReaderEmpty, nil
}

func (r *Reader) FieldDict(field string) (index.FieldDict, error) {
	if r.s.doc == nil {
		return fieldDictEmpty, nil
	}
	fieldSortedTerms, err := r.s.doc.SortedTermsForField(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictEmpty, nil
	}
	return NewFieldDictWithTerms(fieldSortedTerms, nil), nil
}

func (r *Reader) FieldDictRange(field string, startTerm, endTerm []byte) (index.FieldDict, error) {
	if r.s.doc == nil {
		return fieldDictEmpty, nil
	}
	fieldSortedTerms, err := r.s.doc.SortedTermsForField(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictEmpty, nil
	}
	startIdx := sort.SearchStrings(fieldSortedTerms, string(startTerm))
	endTermStr := string(endTerm)
	endIdx := sort.SearchStrings(fieldSortedTerms[startIdx:], endTermStr)
	endIdx += startIdx
	// fix up inclusive end (required by bleve API)
	if endIdx < len(fieldSortedTerms) && fieldSortedTerms[endIdx] == endTermStr {
		endIdx++
	}
	return NewFieldDictWithTerms(fieldSortedTerms[startIdx:endIdx], nil), nil
}

func (r *Reader) FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error) {
	if r.s.doc == nil {
		return fieldDictEmpty, nil
	}
	fieldSortedTerms, err := r.s.doc.SortedTermsForField(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictEmpty, nil
	}
	prefixStr := string(termPrefix)
	startIdx := sort.SearchStrings(fieldSortedTerms, prefixStr)
	rest := fieldSortedTerms[startIdx:]
	endIdx := sort.Search(len(rest), func(i int) bool {
		return !strings.HasPrefix(rest[i], prefixStr)
	})
	return NewFieldDictWithTerms(rest[:endIdx], fieldDictPrefix(prefixStr)), nil
}

func automatonMatch(la vellum.Automaton, termStr string) bool {
	state := la.Start()
	for i := range []byte(termStr) {
		state = la.Accept(state, termStr[i])
		if !la.CanMatch(state) {
			return false
		}
	}
	return la.IsMatch(state)
}

func (r *Reader) FieldDictRegexp(field, regexStr string) (index.FieldDict, error) {
	regex, cached := r.velregCache[regexStr]
	if !cached {
		var err error
		regex, err = velreg.New(regexStr)
		if err != nil {
			return nil, fmt.Errorf("error compiling regexp: %v", err)
		}
		r.velregCache[regexStr] = regex
	}
	if r.s.doc == nil {
		return fieldDictEmpty, nil
	}
	fieldSortedTerms, err := r.s.doc.SortedTermsForField(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictEmpty, nil
	}
	return NewFieldDictWithTerms(fieldSortedTerms, func(s string) bool {
		return automatonMatch(regex, s)
	}), nil
}

func (r *Reader) FieldDictFuzzy(field, term string, fuzziness int, prefix string) (index.FieldDict, error) {
	if r.s.doc == nil {
		return fieldDictEmpty, nil
	}
	fieldSortedTerms, err := r.s.doc.SortedTermsForField(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictEmpty, nil
	}
	return NewFieldDictWithTerms(fieldSortedTerms, func(indexTerm string) bool {
		var dist int
		var exceeded bool
		dist, exceeded, r.levSlice = LevenshteinDistanceMaxReuseSlice(term, indexTerm, fuzziness, r.levSlice)
		if dist <= fuzziness && !exceeded {
			return true
		}
		return false
	}), nil
}

func (r *Reader) FieldDictContains(field string) (index.FieldDictContains, error) {
	if r.s.doc == nil {
		return fieldDictContainsEmpty, nil
	}
	atf, _, err := r.s.doc.TokenFreqsAndLen(field)
	if err != nil {
		// only error is field doesn't exist in doc
		return fieldDictContainsEmpty, nil
	}
	return NewFieldDictContainsFromTokenFrequencies(atf), nil
}

func (r *Reader) Document(id string) (index.Document, error) {
	if r.s.doc.doc.ID() == id {
		return r.s.doc.doc, nil
	}
	return nil, fmt.Errorf("document not found")
}

func (r *Reader) DocValueReader(fields []string) (index.DocValueReader, error) {
	return &DocValueReader{
		r:      r,
		fields: fields,
	}, nil
}

func (r *Reader) Fields() ([]string, error) {
	if r.s.doc != nil {
		return r.s.doc.Fields(), nil
	}
	return nil, nil
}

func (r *Reader) GetInternal(key []byte) ([]byte, error) {
	return r.s.internal[string(key)], nil
}

func (r *Reader) DocCount() (uint64, error) {
	if r.s.doc != nil {
		return 1, nil
	}
	return 0, nil
}

func (r *Reader) ExternalID(id index.IndexInternalID) (string, error) {
	if bytes.Equal(id, internalDocID) {
		return r.s.doc.doc.ID(), nil
	}
	return "", fmt.Errorf("no such document with internal id: '%v'", id)
}

func (r *Reader) InternalID(id string) (index.IndexInternalID, error) {
	if id == r.s.doc.doc.ID() {
		return internalDocID, nil
	}
	return nil, fmt.Errorf("no such document with external id: %s", id)
}

func (r *Reader) Close() error {
	return nil
}
