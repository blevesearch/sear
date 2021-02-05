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
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func TestTermFieldReader(t *testing.T) {
	// first test empty
	var tfr *TermFieldReader = termFieldReaderEmpty

	tfd, err := tfr.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	if tfd != nil {
		t.Fatalf("expected nil, got %#v", tfd)
	}

	tfd, err = tfr.Advance(internalDocID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tfd != nil {
		t.Fatalf("expected nil, got %#v", tfd)
	}

	// now test real
	tf := &index.TokenFreq{
		Term: []byte("real"),
		Locations: []*index.TokenLocation{
			{
				Field:          "f",
				ArrayPositions: nil,
				Start:          0,
				End:            4,
				Position:       1,
			},
			{
				Field:          "f",
				ArrayPositions: nil,
				Start:          5,
				End:            9,
				Position:       2,
			},
			{
				Field:          "f",
				ArrayPositions: nil,
				Start:          10,
				End:            14,
				Position:       3,
			},
		},
	}
	tf.SetFrequency(3)
	tfr = NewTermFieldReaderFromTokenFreqAndLen(tf, 3, true, true, true)
	tfd, err = tfr.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	expectTfd := &index.TermFieldDoc{
		Term: "real",
		ID:   internalDocID,
		Freq: 3,
		Norm: normForLen(3),
		Vectors: []*index.TermFieldVector{
			{
				Field:          "f",
				ArrayPositions: nil,
				Pos:            1,
				Start:          0,
				End:            4,
			},
			{
				Field:          "f",
				ArrayPositions: nil,
				Pos:            2,
				Start:          5,
				End:            9,
			},
			{
				Field:          "f",
				ArrayPositions: nil,
				Pos:            3,
				Start:          10,
				End:            14,
			},
		},
	}

	if !reflect.DeepEqual(tfd, expectTfd) {
		t.Errorf("expected %#v, got %#v", expectTfd, tfd)
	}

	// next again should get nothing
	tfd, err = tfr.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	if tfd != nil {
		t.Fatalf("expected nil, got %#v", tfd)
	}

	// repeat again, but use advance to get there
	tfr = NewTermFieldReaderFromTokenFreqAndLen(tf, 3, true, true, true)
	tfd, err = tfr.Advance(internalDocID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tfd, expectTfd) {
		t.Errorf("expected %#v, got %#v", expectTfd, tfd)
	}

	// repeat again, but use advance to some other internal id
	tfr = NewTermFieldReaderFromTokenFreqAndLen(tf, 3, true, true, true)
	tfd, err = tfr.Advance([]byte{0x1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tfd != nil {
		t.Fatalf("expected nil, got %#v", tfd)
	}

	err = tfr.Close()
	if err != nil {
		t.Fatalf("error closing term field reader: %v", err)
	}
}
