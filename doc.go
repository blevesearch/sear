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
	"sort"

	index "github.com/blevesearch/bleve_index_api"
)

type Document struct {
	doc index.Document

	// always built during analysis
	fieldIndexes    map[string]int
	fieldNames      []string
	fieldTokenFreqs []index.TokenFrequencies
	fieldLens       []int
	vectorDims      []int // applicable to vector fields only

	// docvalues which are not part of the field's analyzed terms, currently
	// only the encoded shape of a geoshape field
	fieldExtraDocValues [][][]byte

	// deferred build and cache
	sortedTerms map[string][]string
}

func NewDocument() *Document {
	return &Document{
		sortedTerms: make(map[string][]string),
	}
}

func (d *Document) fieldIndex(name string) (int, error) {
	if idx, ok := d.fieldIndexes[name]; ok {
		return idx, nil
	}
	return 0, fmt.Errorf("no field named: %s", name)
}

func (d *Document) newField(field index.Field) {
	af := field.AnalyzedTokenFrequencies()

	// bleve analysis will leave field empty for non-composite fields, fix that here
	for _, tf := range af {
		for _, loc := range tf.Locations {
			if loc.Field == "" {
				loc.Field = field.Name()
			}
		}
	}

	fieldIdx, exists := d.fieldIndexes[field.Name()]
	if !exists {
		fieldIdx = len(d.fieldTokenFreqs)
		d.fieldIndexes[field.Name()] = fieldIdx
		d.fieldNames = append(d.fieldNames, field.Name())
		d.fieldTokenFreqs = append(d.fieldTokenFreqs, af)
		d.fieldLens = append(d.fieldLens, field.AnalyzedLength())
		d.vectorDims = append(d.vectorDims, d.interpretVectorIfApplicable(field))
		d.fieldExtraDocValues = append(d.fieldExtraDocValues, nil)
	} else {
		d.fieldTokenFreqs[fieldIdx].MergeAll(field.Name(), af)
		d.fieldLens[fieldIdx] += field.AnalyzedLength()
	}

	// a geoshape field keeps the encoded shape out of its analyzed terms, it is
	// only reachable through the field's docvalues, which is where bleve's
	// geoshape relation filter looks for it.
	if gsf, ok := field.(index.GeoShapeField); ok {
		d.fieldExtraDocValues[fieldIdx] = append(d.fieldExtraDocValues[fieldIdx],
			gsf.EncodedShape())
	}
}

func (d *Document) analyze() {
	// first visit regular fields
	d.doc.VisitFields(func(field index.Field) {
		if field.Options().IsIndexed() {
			field.Analyze()

			d.newField(field)

			if d.doc.HasComposite() && field.Name() != "_id" {
				// see if any of the composite fields need this
				d.doc.VisitComposite(func(cf index.CompositeField) {
					cf.Compose(field.Name(), field.AnalyzedLength(), field.AnalyzedTokenFrequencies())
				})

				// Since the encoded geoShape is only necessary within the doc values
				// of the geoShapeField, it has been removed from the field's term dictionary.
				// However, '_all' field uses its term dictionary as its docValues, so it
				// becomes necessary to add the geoShape into the '_all' field's term dictionary
				if f, ok := field.(index.GeoShapeField); ok {
					d.doc.VisitComposite(func(cf index.CompositeField) {
						geoshape := f.EncodedShape()
						cf.Compose(field.Name(), 1, index.TokenFrequencies{
							string(geoshape): &index.TokenFreq{
								Term: geoshape,
								Locations: []*index.TokenLocation{
									{
										Start:    0,
										End:      len(geoshape),
										Position: 1,
									},
								},
							},
						})
					})
				}
			}
		}
	})

	// now add the composite fields
	d.doc.VisitComposite(func(field index.CompositeField) {
		d.newField(field)
	})
}

func (d *Document) Reset(doc index.Document) {
	// clear analysis
	d.fieldIndexes = make(map[string]int, len(d.fieldNames))
	d.fieldNames = d.fieldNames[:0]
	d.fieldTokenFreqs = d.fieldTokenFreqs[:0]
	d.fieldLens = d.fieldLens[:0]
	d.vectorDims = d.vectorDims[:0]
	d.fieldExtraDocValues = d.fieldExtraDocValues[:0]

	// clear cache
	for k := range d.sortedTerms {
		d.sortedTerms[k] = d.sortedTerms[k][:0]
	}

	// init new doc
	d.doc = doc
	d.analyze()
}

func (d *Document) Fields() []string {
	return d.fieldNames
}

func (d *Document) SortedTermsForField(fieldName string) ([]string, error) {
	fieldIdx, err := d.fieldIndex(fieldName)
	if err != nil {
		return nil, err
	}

	terms, ok := d.sortedTerms[fieldName]
	if ok && len(terms) > 0 {
		return terms, nil
	}

	atf := d.fieldTokenFreqs[fieldIdx]
	for k := range atf {
		terms = append(terms, k)
	}
	sort.Strings(terms)
	d.sortedTerms[fieldName] = terms
	return terms, nil
}

func (d *Document) TokenFreqsAndLen(fieldName string) (index.TokenFrequencies, int, error) {
	fieldIdx, err := d.fieldIndex(fieldName)
	if err != nil {
		return nil, 0, err
	}
	return d.fieldTokenFreqs[fieldIdx], d.fieldLens[fieldIdx], nil
}

func (d *Document) ExtraDocValues(fieldName string) [][]byte {
	fieldIdx, err := d.fieldIndex(fieldName)
	if err != nil {
		return nil
	}
	return d.fieldExtraDocValues[fieldIdx]
}

func (d *Document) VectorDims(fieldName string) (dims int, err error) {
	fieldIdx, err := d.fieldIndex(fieldName)
	if err != nil {
		return 0, err
	}

	return d.vectorDims[fieldIdx], nil
}
