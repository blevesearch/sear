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
	"testing"
)

func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		searchTerm     string
		fuzziness      int
		shouldMatch    []string
		shouldNotMatch []string
	}{
		{
			searchTerm: "cat",
			fuzziness:  1,
			shouldMatch: []string{
				"ca",
				"cab",
				"car",
				"bat",
				"fat",
				"cats",
			},
			shouldNotMatch: []string{
				"c",
				"catss",
				"catsss",
				"bar",
			},
		},
		{
			searchTerm: "cat",
			fuzziness:  2,
			shouldMatch: []string{
				"ca",
				"cab",
				"car",
				"bat",
				"fat",
				"cats",
				"catss",
				"bar",
				"c",
				"tac",
			},
			shouldNotMatch: []string{
				"catsss",
				"tacs",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s-%d", test.searchTerm, test.fuzziness), func(t *testing.T) {
			for _, sm := range test.shouldMatch {
				dist, exceeded, _ := levenshteinDistanceMaxReuseSlice(test.searchTerm, sm, test.fuzziness, nil)
				if dist > test.fuzziness || exceeded {
					t.Errorf("expected %s to match, did not", sm)
				}
			}
			for _, snm := range test.shouldNotMatch {
				dist, exceeded, _ := levenshteinDistanceMaxReuseSlice(test.searchTerm, snm, test.fuzziness, nil)
				if dist <= test.fuzziness && !exceeded {
					t.Errorf("expected %s not to match, did", snm)
				}
			}
		})
	}
}
