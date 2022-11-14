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
	"reflect"
	"strings"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func TestIndexCrud(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		cerr := idx.Close()
		if cerr != nil {
			t.Fatalf("error closing index: %v", cerr)
		}
	}()

	mapAndUpdateDocument(t, idx, "a", map[string]interface{}{
		"name":   "marty",
		"title":  "software developer",
		"slogan": "code match",
	})

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	count, err := reader.DocCount()
	if err != nil {
		t.Fatalf("error getting doc count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected doc count 1, got %d", count)
	}

	// field dict for field that doesn't exist
	fd, err := reader.FieldDict("invalidfield")
	if err != nil {
		t.Errorf("error getting field dictionary for invalidfield: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = reader.FieldDict("slogan")
	if err != nil {
		t.Errorf("error getting field dictionary for slogan: %v", err)
	}
	assertTermDictionary(t, fd, []string{"code", "match"})

	// tfr for field that doesn't exist
	tfr, err := reader.TermFieldReader(nil, []byte("marty"), "invalidfield", true, true, true)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	assertTermFieldReaderEmpty(t, tfr)

	tfr, err = reader.TermFieldReader(nil, []byte("marty"), "name", true, true, true)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	assertTermFieldReader(t, tfr, []*index.TermFieldDoc{
		{
			Term: "marty",
			ID:   internalDocID,
			Freq: 1,
			Norm: normForLen(1),
			Vectors: []*index.TermFieldVector{
				{
					Field:          "name",
					ArrayPositions: []uint64{},
					Pos:            1,
					Start:          0,
					End:            5,
				},
			},
		},
	})

	// test round trip doc ids
	extID, err := reader.ExternalID(internalDocID)
	if err != nil {
		t.Errorf("error getting external id: %v", err)
	}
	if extID != "a" {
		t.Errorf("expected external id to be 'a', got '%s'", extID)
	}
	intID, err := reader.InternalID(extID)
	if err != nil {
		t.Errorf("error getting internal id: %v", err)
	}
	if !bytes.Equal(intID, internalDocID) {
		t.Errorf("expected external back to internal ID to match, did not. got %v orig %v", intID, internalDocID)
	}

	// index a new doc
	mapAndUpdateDocument(t, idx, "b", map[string]interface{}{
		"name":   "tiger",
		"title":  "zoo",
		"slogan": "golf match",
	})

	// make sure previous name fails
	tfr, err = reader.TermFieldReader(nil, []byte("marty"), "name", true, true, true)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	assertTermFieldReaderEmpty(t, tfr)

	// now look for what we expect to find
	tfr, err = reader.TermFieldReader(nil, []byte("tiger"), "name", true, true, true)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	assertTermFieldReader(t, tfr, []*index.TermFieldDoc{
		{
			Term: "tiger",
			ID:   internalDocID,
			Freq: 1,
			Norm: normForLen(1),
			Vectors: []*index.TermFieldVector{
				{
					Field:          "name",
					ArrayPositions: []uint64{},
					Pos:            1,
					Start:          0,
					End:            5,
				},
			},
		},
	})

	// check we get correct ext id for this new doc
	extID, err = reader.ExternalID(internalDocID)
	if err != nil {
		t.Errorf("error getting external id: %v", err)
	}
	if extID != "b" {
		t.Errorf("expected external id to be 'b', got '%s'", extID)
	}

	// try doc id reader
	docIDReaderAll, err := reader.DocIDReaderAll()
	if err != nil {
		t.Fatalf("error getting doc id reader all: %v", err)
	}
	assertDocIDReader(t, docIDReaderAll, [][]byte{internalDocID})

	// try doc id reader only (including our id)
	docIDReaderOnly, err := reader.DocIDReaderOnly([]string{"b"})
	if err != nil {
		t.Fatalf("error getting doc id reader all: %v", err)
	}
	assertDocIDReader(t, docIDReaderOnly, [][]byte{internalDocID})

	// try doc id reader only (without our id)
	docIDReaderOnly, err = reader.DocIDReaderOnly([]string{"c"})
	if err != nil {
		t.Fatalf("error getting doc id reader all: %v", err)
	}
	assertDocIDReaderEmpty(t, docIDReaderOnly)

	// try doc id reader only (including our id and others)
	docIDReaderOnly, err = reader.DocIDReaderOnly([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("error getting doc id reader all: %v", err)
	}
	assertDocIDReader(t, docIDReaderOnly, [][]byte{internalDocID})

	// index a new doc
	mapAndUpdateDocument(t, idx, "c", map[string]interface{}{
		"name":   "snake",
		"title":  "college",
		"slogan": "the quick brown fox jumps over the lazy dog",
	})

	// prefix for field that doesn't exist
	fd, err = reader.FieldDictPrefix("invalidfield", []byte("q"))
	if err != nil {
		t.Fatalf("error getting field dictionary prefix: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = reader.FieldDictPrefix("slogan", []byte("q"))
	if err != nil {
		t.Fatalf("error getting field dictionary prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"quick"})

	// range for field that doesn't exist
	fd, err = reader.FieldDictRange("invalidfield", []byte("a"), []byte("e"))
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = reader.FieldDictRange("slogan", []byte("a"), []byte("e"))
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"brown", "dog"})

	readerRegexp := reader.(index.IndexReaderRegexp)

	// regexp for field that doesn't exist
	fd, err = readerRegexp.FieldDictRegexp("invalidfield", "jum.*")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = readerRegexp.FieldDictRegexp("slogan", "jum.*")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"jumps"})

	readerFuzzy := reader.(index.IndexReaderFuzzy)

	// fuzzy for field that doesn't exist
	fd, err = readerFuzzy.FieldDictFuzzy("invalidfield", "browm", 1, "")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = readerFuzzy.FieldDictFuzzy("slogan", "browm", 1, "")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"brown"})

	fd, err = readerFuzzy.FieldDictFuzzy("slogan", "braun", 2, "")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"brown"})

	readerContains := reader.(index.IndexReaderContains)
	fdc, err := readerContains.FieldDictContains("slogan")
	if err != nil {
		t.Fatalf("error getting field dict contains: %v", err)
	}

	found, err := fdc.Contains([]byte("quick"))
	if err != nil {
		t.Fatalf("error invoking field dict contains: %v", err)
	}
	if !found {
		t.Errorf("expected to find term 'quick', did not")
	}
	found, err = fdc.Contains([]byte("quack"))
	if err != nil {
		t.Fatalf("error invoking field dict contains: %v", err)
	}
	if found {
		t.Errorf("expected not to find term 'quack', did not")
	}

	dvr, err := reader.DocValueReader([]string{"slogan"})
	if err != nil {
		t.Fatalf("error getting doc value reader: %v", err)
	}
	var termsSeen []string
	termsExpected := []string{"quick", "brown", "fox", "jumps", "lazy", "dog", "the", "over"}
	err = dvr.VisitDocValues(internalDocID, func(field string, term []byte) {
		if field != "slogan" {
			t.Fatalf("unexpected field in visitor: %s", field)
		}
		termsSeen = append(termsSeen, string(term))
	})
	if err != nil {
		t.Fatalf("error visiting doc values: %v", err)
	}
	assertAllAndOnlyValues(t, termsExpected, termsSeen)

	fields, err := reader.Fields()
	if err != nil {
		t.Fatalf("error getting index fields: %v", err)
	}
	fieldsExpected := []string{"name", "title", "slogan", "_all"}
	assertAllAndOnlyValues(t, fieldsExpected, fields)

	// try using internal storage
	err = idx.SetInternal([]byte("intk1"), []byte("intv1"))
	if err != nil {
		t.Errorf("error setting internal value: %v", err)
	}
	intV, err := reader.GetInternal([]byte("intk1"))
	if err != nil {
		t.Errorf("error getting internal value: %v", err)
	}
	if !bytes.Equal(intV, []byte("intv1")) {
		t.Errorf("expected internal value: %v, got %v", []byte("intv1"), intV)
	}
	err = idx.DeleteInternal([]byte("intk1"))
	if err != nil {
		t.Errorf("error deleting internal: %v", err)
	}
	intV, err = reader.GetInternal([]byte("intk1"))
	if err != nil {
		t.Errorf("error getting internal value: %v", err)
	}
	if intV != nil {
		t.Errorf("expected nil internal value, got %v", intV)
	}
}

func assertAllAndOnlyValues(t *testing.T, expect, seen []string) {
	expectMap := make(map[string]struct{})
	for _, v := range expect {
		expectMap[v] = struct{}{}
	}
	for _, v := range seen {
		if _, ok := expectMap[v]; ok {
			delete(expectMap, v)
		} else {
			t.Errorf("did not expect to see: %s", v)
		}
	}
	if len(expectMap) > 0 {
		for k := range expectMap {
			t.Errorf("did not see expected: %s", k)
		}
	}
}

func assertTermFieldReaderEmpty(t *testing.T, tfr index.TermFieldReader) {
	assertTermFieldReader(t, tfr, nil)
}

func assertTermFieldReader(t *testing.T, tfr index.TermFieldReader, expected []*index.TermFieldDoc) {
	var actual []*index.TermFieldDoc
	tfd, err := tfr.Next(nil)
	for err == nil && tfd != nil {
		actual = append(actual, tfd)
		tfd, err = tfr.Next(tfd)
	}
	if err != nil {
		t.Fatalf("error getting next tfd: %v", err)
	}

	if len(actual) != int(tfr.Count()) {
		t.Errorf("expected term field reader count: %d to match number of items returned: %d",
			len(actual), tfr.Count())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected %#v, got %#v", expected, actual)
	}
}

func assertDocIDReaderEmpty(t *testing.T, reader index.DocIDReader) {
	assertDocIDReader(t, reader, nil)
}

func assertDocIDReader(t *testing.T, reader index.DocIDReader, expected [][]byte) {
	var internalIdsSeen [][]byte
	internalID, err := reader.Next()
	for err == nil && internalID != nil {
		internalIdsSeen = append(internalIdsSeen, internalID)
		internalID, err = reader.Next()
	}
	if !reflect.DeepEqual(expected, internalIdsSeen) {
		t.Fatalf("expected: %v, got %v", expected, internalIdsSeen)
	}
}

func assertTermDictionaryEmpty(t *testing.T, dict index.FieldDict) {
	assertTermDictionary(t, dict, nil)
}

func assertTermDictionary(t *testing.T, dict index.FieldDict, expectedTerms []string) {
	var terms []string
	var next *index.DictEntry
	next, err := dict.Next()
	for err == nil && next != nil {
		terms = append(terms, next.Term)
		next, err = dict.Next()
	}
	if err != nil {
		t.Fatalf("error iterating term dictionary: %v", err)
	}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected terms: %#v, got %#v", expectedTerms, terms)
	}
}

func assertEmptyIndex(t *testing.T, reader index.IndexReader) {
	count, err := reader.DocCount()
	if err != nil {
		t.Fatalf("error getting count: %d", count)
	}
	if count != 0 {
		t.Errorf("expected doc count 0, got %d", count)
	}

	tfr, err := reader.TermFieldReader(nil, []byte("b"), "field", true, true, true)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	assertTermFieldReaderEmpty(t, tfr)

	dr, err := reader.DocIDReaderAll()
	if err != nil {
		t.Fatalf("error getting doc id reader all: %v", err)
	}
	assertDocIDReaderEmpty(t, dr)

	dr, err = reader.DocIDReaderOnly([]string{"a"})
	if err != nil {
		t.Fatalf("error getting doc id reader only: %v", err)
	}
	assertDocIDReaderEmpty(t, dr)

	fd, err := reader.FieldDict("field")
	if err != nil {
		t.Fatalf("error getting field dict: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = reader.FieldDictRange("field", []byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("error getting field dict: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	fd, err = reader.FieldDictPrefix("field", []byte("b"))
	if err != nil {
		t.Fatalf("error getting field dict: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	readerRegexp := reader.(index.IndexReaderRegexp)
	fd, err = readerRegexp.FieldDictRegexp("field", "b.*")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)

	readerFuzzy := reader.(index.IndexReaderFuzzy)
	fd, err = readerFuzzy.FieldDictFuzzy("field", "b", 1, "")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionaryEmpty(t, fd)
}

func TestEmpty(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	// check empty
	assertEmptyIndex(t, reader)

	// now index a doc
	mapAndUpdateDocument(t, idx, "a", map[string]interface{}{
		"field": "b",
	})

	// delete it
	err = idx.Delete("a")
	if err != nil {
		t.Fatalf("error deleting doc: %v", err)
	}

	// check again
	assertEmptyIndex(t, reader)
}

type fatalfable interface {
	Fatalf(format string, args ...interface{})
}

func mapAndUpdateDocument(t fatalfable, idx index.Index, id string, doc map[string]interface{}) {
	bleveDoc := newTestDoc(id)
	for k, v := range doc {
		if vstr, ok := v.(string); ok {
			bleveDoc.AddField(newTestField(k, []byte(vstr)))
		}
		if vstrArr, ok := v.([]string); ok {
			for _, vstr := range vstrArr {
				bleveDoc.AddField(newTestField(k, []byte(vstr)))
			}
		}
	}

	err := idx.Update(bleveDoc)
	if err != nil {
		t.Fatalf("error indexing document: %v", err)
	}
}

const origBleveField = `There are three characteristics of liquids which are relevant to the discussion of a BLEVE:
If a liquid in a sealed container is boiled, the pressure inside the container increases. As the liquid changes to a gas it expands - this expansion in a vented container would cause the gas and liquid to take up more space. In a sealed container the gas and liquid are not able to take up more space and so the pressure rises. Pressurized vessels containing liquids can reach an equilibrium where the liquid stops boiling and the pressure stops rising. This occurs when no more heat is being added to the system (either because it has reached ambient temperature or has had a heat source removed).
The boiling temperature of a liquid is dependent on pressure - high pressures will yield high boiling temperatures, and low pressures will yield low boiling temperatures. A common simple experiment is to place a cup of water in a vacuum chamber, and then reduce the pressure in the chamber until the water boils. By reducing the pressure the water will boil even at room temperature. This works both ways - if the pressure is increased beyond normal atmospheric pressures, the boiling of hot water could be suppressed far beyond normal temperatures. The cooling system of a modern internal combustion engine is a real-world example.
When a liquid boils it turns into a gas. The resulting gas takes up far more space than the liquid did.
Typically, a BLEVE starts with a container of liquid which is held above its normal, atmospheric-pressure boiling temperature. Many substances normally stored as liquids, such as CO2, oxygen, and other similar industrial gases have boiling temperatures, at atmospheric pressure, far below room temperature. In the case of water, a BLEVE could occur if a pressurized chamber of water is heated far beyond the standard 100 °C (212 °F). That container, because the boiling water pressurizes it, is capable of holding liquid water at very high temperatures.
If the pressurized vessel, containing liquid at high temperature (which may be room temperature, depending on the substance) ruptures, the pressure which prevents the liquid from boiling is lost. If the rupture is catastrophic, where the vessel is immediately incapable of holding any pressure at all, then there suddenly exists a large mass of liquid which is at very high temperature and very low pressure. This causes the entire volume of liquid to instantaneously boil, which in turn causes an extremely rapid expansion. Depending on temperatures, pressures and the substance involved, that expansion may be so rapid that it can be classified as an explosion, fully capable of inflicting severe damage on its surroundings.`

var largeBleveField = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(origBleveField, ",", ""), ":", ""), ".", "")

func TestFieldDictRange(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mapAndUpdateDocument(t, idx, "bleve", map[string]interface{}{
		"title": "Bleve",
		"body":  largeBleveField,
	})

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	// test range ending with last value in list
	fd, err := reader.FieldDictRange("body", []byte("water"), []byte("yield"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"water", "ways", "when", "where", "which", "will", "with", "works", "would", "yield"})

	// test range going past last value in list
	fd, err = reader.FieldDictRange("body", []byte("water"), []byte("z"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"water", "ways", "when", "where", "which", "will", "with", "works", "would", "yield"})

	// test range starting with first value in lists
	fd, err = reader.FieldDictRange("body", []byte("a"), []byte("ball"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"a", "able", "above", "added", "all", "ambient", "an", "and", "any", "are", "as", "at", "atmospheric", "atmospheric-pressure"})

	// test range starting before first value in lists
	fd, err = reader.FieldDictRange("body", []byte("_"), []byte("ball"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"a", "able", "above", "added", "all", "ambient", "an", "and", "any", "are", "as", "at", "atmospheric", "atmospheric-pressure"})

	// test range with start and end in the list
	fd, err = reader.FieldDictRange("body", []byte("large"), []byte("low"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"large", "liquid", "liquids", "lost", "low"})

	// test range with start and end inside the list range, but not actually in the list
	fd, err = reader.FieldDictRange("body", []byte("l"), []byte("m"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"large", "liquid", "liquids", "lost", "low"})
}

func TestLarger(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mapAndUpdateDocument(t, idx, "bleve", map[string]interface{}{
		"title": "Bleve",
		"body":  largeBleveField,
	})

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	fd, err := reader.FieldDictPrefix("body", []byte("b"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"be", "because", "being", "below", "beyond", "bleve", "boil", "boiled", "boiling", "boils", "both", "by"})

	fd, err = reader.FieldDictRange("body", []byte("water"), []byte("world"))
	if err != nil {
		t.Fatalf("error getting field dict prefix: %v", err)
	}
	assertTermDictionary(t, fd, []string{"water", "ways", "when", "where", "which", "will", "with", "works"})

	readerRegexp := reader.(index.IndexReaderRegexp)
	fd, err = readerRegexp.FieldDictRegexp("body", "li.*")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"liquid", "liquids"})

	readerFuzzy := reader.(index.IndexReaderFuzzy)
	fd, err = readerFuzzy.FieldDictFuzzy("body", "gas", 2, "")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"a", "an", "as", "at", "can", "case", "far", "gas", "gases", "had", "has", "is", "its", "mass", "may", "ways"})
}

func TestMB47265(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mapAndUpdateDocument(t, idx, "bleve", map[string]interface{}{
		"title": "Bleve",
		"body":  []string{"one", "two", "three"},
	})

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	for _, term := range []string{"one", "two", "three"} {
		tfr, err := reader.TermFieldReader(nil, []byte(term), "body", false, false, false)
		if err != nil {
			t.Fatalf("error setting up term field reader: %v, ", err)
		}

		if tfr.Count() != 1 {
			t.Errorf("error unexpected count for term field reader on term: %v", term)
		}
	}
}

func TestMB47473(t *testing.T) {
	idx, err := New("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mapAndUpdateDocument(t, idx, "bleve", map[string]interface{}{
		"title": "Bleve",
		"body":  []string{"ko", "kó"},
	})

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}

	readerRegexp := reader.(index.IndexReaderRegexp)
	fd, err := readerRegexp.FieldDictRegexp("body", "k.*")
	if err != nil {
		t.Fatalf("error getting field dictionary range: %v", err)
	}
	assertTermDictionary(t, fd, []string{"ko", "kó"})
}

func createBenchmarkIndexReader(b *testing.B) index.IndexReader {
	idx, err := New("", nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	mapAndUpdateDocument(b, idx, "bleve", map[string]interface{}{
		"title": "Bleve",
		"body":  largeBleveField,
	})

	reader, err := idx.Reader()
	if err != nil {
		b.Fatalf("error getting index reader: %v", err)
	}

	return reader
}

func BenchmarkFieldDictPrefix(b *testing.B) {
	reader := createBenchmarkIndexReader(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict, err := reader.FieldDictPrefix("body", []byte("b"))
		if err != nil {
			b.Fatalf("error getting field dict prefix: %v", err)
		}
		var next *index.DictEntry
		next, err = dict.Next()
		for err == nil && next != nil {
			next, err = dict.Next()
		}
		if err != nil {
			b.Fatalf("error iterating term dictionary: %v", err)
		}
	}
}

func BenchmarkFieldDictRange(b *testing.B) {
	reader := createBenchmarkIndexReader(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict, err := reader.FieldDictRange("body", []byte("water"), []byte("world"))
		if err != nil {
			b.Fatalf("error getting field dict prefix: %v", err)
		}
		var next *index.DictEntry
		next, err = dict.Next()
		for err == nil && next != nil {
			next, err = dict.Next()
		}
		if err != nil {
			b.Fatalf("error iterating term dictionary: %v", err)
		}
	}
}

func BenchmarkFieldDictRegexp(b *testing.B) {
	reader := createBenchmarkIndexReader(b)
	readerRegexp := reader.(index.IndexReaderRegexp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict, err := readerRegexp.FieldDictRegexp("body", "li.*")
		if err != nil {
			b.Fatalf("error getting field dict prefix: %v", err)
		}
		var next *index.DictEntry
		next, err = dict.Next()
		for err == nil && next != nil {
			next, err = dict.Next()
		}
		if err != nil {
			b.Fatalf("error iterating term dictionary: %v", err)
		}
	}
}

func BenchmarkFieldDictFuzzy(b *testing.B) {
	reader := createBenchmarkIndexReader(b)
	readerFuzzy := reader.(index.IndexReaderFuzzy)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict, err := readerFuzzy.FieldDictFuzzy("body", "gas", 2, "")
		if err != nil {
			b.Fatalf("error getting field dict prefix: %v", err)
		}
		var next *index.DictEntry
		next, err = dict.Next()
		for err == nil && next != nil {
			next, err = dict.Next()
		}
		if err != nil {
			b.Fatalf("error iterating term dictionary: %v", err)
		}
	}
}
