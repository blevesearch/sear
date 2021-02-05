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

func TestFieldDict(t *testing.T) {
	tests := []struct {
		name   string
		fd     *FieldDict
		expect []string
	}{
		{
			name:   "empty",
			fd:     fieldDictEmpty,
			expect: nil,
		},
		{
			name:   "all",
			fd:     NewFieldDictWithTerms([]string{"a", "b", "c"}, nil),
			expect: []string{"a", "b", "c"},
		},
		{
			name: "prefix",
			fd: NewFieldDictWithTerms(
				[]string{
					"able",
					"baker",
					"ball",
					"basket",
					"bed",
					"charlie",
					"dog",
				},
				fieldDictPrefix("ba")),
			expect: []string{
				"baker",
				"ball",
				"basket",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var actual []string
			next, err := test.fd.Next()
			for err == nil && next != nil {
				actual = append(actual, next.Term)
				next, err = test.fd.Next()
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.expect, actual) {
				t.Errorf("expected: %v got %v", test.expect, actual)
			}
		})
	}
}

func TestFieldDictContains(t *testing.T) {
	tests := []struct {
		name           string
		fd             *FieldDictContains
		expectFound    []string
		expectNotFound []string
	}{
		{
			name:           "empty",
			fd:             fieldDictContainsEmpty,
			expectFound:    nil,
			expectNotFound: []string{"a", "b", "c", "d"},
		},
		{
			name: "token-freq-lookup",
			fd: NewFieldDictContainsFromTokenFrequencies(
				map[string]*index.TokenFreq{
					"a": nil,
					"b": nil,
					"d": nil,
				}),
			expectFound:    []string{"a", "b", "d"},
			expectNotFound: []string{"c"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for _, term := range test.expectFound {
				contains, err := test.fd.Contains([]byte(term))
				if err != nil {
					t.Fatal(err)
				}
				if !contains {
					t.Errorf("expected to find %s, did not", term)
				}
			}
			for _, term := range test.expectNotFound {
				contains, err := test.fd.Contains([]byte(term))
				if err != nil {
					t.Fatal(err)
				}
				if contains {
					t.Errorf("expected to not find %s, did", term)
				}
			}
		})
	}
}
