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

func levenshteinDistanceMaxReuseSlice(a, b string, max int, d []int) (dist int, exceeded bool, reuse []int) {
	la := len(a)
	lb := len(b)

	ld := la - lb
	if ld < 0 {
		ld = -ld
	}
	if ld > max {
		return max, true, d
	}

	if cap(d) < la+1 {
		d = make([]int, la+1)
	}
	d = d[:la+1]

	var lastdiag, olddiag, temp int

	for i := 1; i <= la; i++ {
		d[i] = i
	}
	for i := 1; i <= lb; i++ {
		d[0] = i
		lastdiag = i - 1
		rowmin := max + 1
		for j := 1; j <= la; j++ {
			olddiag = d[j]
			min := d[j] + 1
			if (d[j-1] + 1) < min {
				min = d[j-1] + 1
			}
			if a[j-1] == b[i-1] {
				temp = 0
			} else {
				temp = 1
			}
			if (lastdiag + temp) < min {
				min = lastdiag + temp
			}
			if min < rowmin {
				rowmin = min
			}
			d[j] = min

			lastdiag = olddiag
		}
		// after each row if rowmin isn't less than max stop
		if rowmin > max {
			return max, true, d
		}
	}
	return d[la], false, d
}
