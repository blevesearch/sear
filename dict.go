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
	"strings"

	index "github.com/blevesearch/bleve_index_api"
)

func fieldDictPrefix(prefix string) func(string) bool {
	return func(term string) bool {
		return strings.HasPrefix(term, prefix)
	}
}

type FieldDict struct {
	terms       []string
	index       int
	includeFunc func(term string) bool

	next index.DictEntry
}

var fieldDictEmpty = NewFieldDictEmpty()

func NewFieldDictEmpty() *FieldDict {
	return &FieldDict{}
}

func NewFieldDictWithTerms(terms []string, include func(string) bool) *FieldDict {
	return &FieldDict{
		terms:       terms,
		includeFunc: include,
	}
}

func (d *FieldDict) Next() (*index.DictEntry, error) {
	for d.index < len(d.terms) {
		// if we need to skip this item increment and continue
		if d.includeFunc != nil && !d.includeFunc(d.terms[d.index]) {
			d.index++
			continue
		}
		d.next.Term = d.terms[d.index]
		d.next.Count = 1

		d.index++
		return &d.next, nil
	}
	return nil, nil
}

func (d *FieldDict) Close() error {
	return nil
}

type FieldDictContains struct {
	atf index.TokenFrequencies
}

var fieldDictContainsEmpty = NewFieldDictContainsEmpty()

func NewFieldDictContainsEmpty() *FieldDictContains {
	return &FieldDictContains{}
}

func NewFieldDictContainsFromTokenFrequencies(atf index.TokenFrequencies) *FieldDictContains {
	return &FieldDictContains{
		atf: atf,
	}
}

func (d *FieldDictContains) Contains(key []byte) (bool, error) {
	if d.atf == nil {
		return false, nil
	}
	if _, ok := d.atf[string(key)]; ok {
		return true, nil
	}
	return false, nil
}
