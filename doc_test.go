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
	"reflect"
	"strings"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func TestDocument(t *testing.T) {
	// the document obj we test with
	doc := NewDocument()

	// start with simple doc
	bleveDoc := newTestDoc("a")
	bleveDoc.AddField(newTestField("name", []byte("marty")))
	doc.Reset(bleveDoc)

	// assert we see expected field
	assertAllAndOnlyValues(t, []string{"name", "_all"}, doc.Fields())

	// now reset to new document
	bleveDoc = newTestDoc("b")
	bleveDoc.AddField(newTestField("project", []byte("bleve")))
	doc.Reset(bleveDoc)

	// assert we see expected field
	assertAllAndOnlyValues(t, []string{"project", "_all"}, doc.Fields())

	// now reset to more realistic document
	bleveDoc = newTestDoc("c")
	bleveDoc.AddField(newTestField("username", []byte("mather")))
	bleveDoc.AddField(newTestField("description", []byte("phrase bleve bleve bleve")))
	doc.Reset(bleveDoc)

	tfs, l, err := doc.TokenFreqsAndLen("description")
	if err != nil {
		t.Fatal(err)
	}
	if l != 4 {
		t.Errorf("expected description field len 4, got %d", l)
	}
	if len(tfs) != 2 {
		t.Errorf("expected description tfs to be len 2, got %d", len(tfs))
		t.Logf("%#v", tfs)
	}
	for term, tf := range tfs {
		switch term {
		case "phrase":
			if tf.Frequency() != 1 {
				t.Errorf("expected 'phrase' to freq 1, got %d", tf.Frequency())
			}
		case "bleve":
			if tf.Frequency() != 3 {
				t.Errorf("expected 'phrase' to freq 3, got %d", tf.Frequency())
			}
		}
	}

	// expect field names not to be sorted yet
	if len(doc.sortedTerms) > 0 {
		t.Errorf("expected no sorted terms yet, got %#v", doc.sortedTerms)
	}

	// get the sorted terms
	st, err := doc.SortedTermsForField("description")
	if err != nil {
		t.Fatal(err)
	}
	expectedSortedTerms := []string{"bleve", "phrase"}
	if !reflect.DeepEqual(st, expectedSortedTerms) {
		t.Errorf("expected sorted terms: %v, got %v", expectedSortedTerms, st)
	}

	if len(doc.sortedTerms) != 1 {
		t.Errorf("expected 1 cached sorted terms, got %#v", doc.sortedTerms)
	}
}

type testDoc struct {
	id         string
	fields     []index.Field
	composites []index.CompositeField
}

func newTestDoc(id string) *testDoc {
	return &testDoc{
		id: id,
		composites: []index.CompositeField{
			&testField{
				name:               "_all",
				analyzedTokenFreqs: make(index.TokenFrequencies),
			},
		},
	}
}

func (t *testDoc) AddField(f index.Field) {
	t.fields = append(t.fields, f)
}

func (t *testDoc) ID() string {
	return t.id
}

func (t *testDoc) Size() int {
	return 0
}

func (t *testDoc) VisitFields(visitor index.FieldVisitor) {
	for _, field := range t.fields {
		visitor(field)
	}
}

func (t *testDoc) VisitComposite(visitor index.CompositeFieldVisitor) {
	for _, composite := range t.composites {
		visitor(composite)
	}
}

func (t *testDoc) HasComposite() bool {
	return len(t.composites) > 0
}

func (t *testDoc) NumPlainTextBytes() uint64 {
	return 0
}

func (t *testDoc) AddIDField() {}

func (t *testDoc) StoredFieldsBytes() uint64 {
	return 0
}

func (t *testDoc) Indexed() bool {
	return true
}

type testField struct {
	name               string
	val                []byte
	ap                 []uint64
	options            index.FieldIndexingOptions
	analyzedLen        int
	analyzedTokenFreqs index.TokenFrequencies
}

func newTestField(name string, val []byte) *testField {
	return &testField{
		name:               name,
		val:                val,
		ap:                 []uint64{},
		options:            index.IndexField | index.DocValues,
		analyzedTokenFreqs: make(index.TokenFrequencies),
	}
}

func (t *testField) Name() string {
	return t.name
}

func (t *testField) Value() []byte {
	return t.val
}

func (t *testField) ArrayPositions() []uint64 {
	return t.ap
}

func (t *testField) EncodedFieldType() byte {
	return 't'
}

func (t *testField) Analyze() {
	tokens := strings.Split(string(t.val), " ")
	var currPos int
	for i, token := range tokens {
		tokenLower := strings.ToLower(token)
		if i != 0 {
			currPos++ // space
		}
		tf, ok := t.analyzedTokenFreqs[tokenLower]
		if ok {
			tf.SetFrequency(tf.Frequency() + 1)
			tf.Locations = append(tf.Locations, &index.TokenLocation{
				Field:          t.name,
				ArrayPositions: t.ap,
				Start:          currPos,
				End:            currPos + len(tokenLower),
				Position:       i + 1,
			})
		} else {
			t.analyzedTokenFreqs[tokenLower] = &index.TokenFreq{
				Term: []byte(tokenLower),
				Locations: []*index.TokenLocation{
					{
						Field:          t.name,
						ArrayPositions: t.ap,
						Start:          currPos,
						End:            currPos + len(tokenLower),
						Position:       i + 1,
					},
				},
			}
			t.analyzedTokenFreqs[tokenLower].SetFrequency(1)
		}
		currPos += len(tokenLower)
	}
	t.analyzedLen = len(tokens)
}

func (t *testField) Options() index.FieldIndexingOptions {
	return t.options
}

func (t *testField) AnalyzedLength() int {
	return t.analyzedLen
}

func (t *testField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return t.analyzedTokenFreqs
}

func (t *testField) NumPlainTextBytes() uint64 {
	return 0
}

func (t *testField) Compose(field string, length int, freq index.TokenFrequencies) {
	t.analyzedLen += length
	t.analyzedTokenFreqs.MergeAll(field, freq)
}
