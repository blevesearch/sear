# sear

[![PkgGoDev](https://pkg.go.dev/badge/github.com/blevesearch/sear)](https://pkg.go.dev/github.com/blevesearch/sear)
[![Tests](https://github.com/blevesearch/sear/workflows/Tests/badge.svg?branch=master&event=push)](https://github.com/blevesearch/sear/actions?query=workflow%3ATests+event%3Apush+branch%3Amaster)
[![Lint](https://github.com/blevesearch/sear/workflows/Lint/badge.svg?branch=master&event=push)](https://github.com/blevesearch/sear/actions?query=workflow%3ALint+event%3Apush+branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/blevesearch/sear/badge.svg)](https://coveralls.io/github/blevesearch/sear)

Sear is a Bleve index implementation designed for efficiently executing searches against a single document .

Why is this useful?  Sometimes, a use case arises where it is useful to be able to answer the question, "would this document have matched this search?"
And frequently we may want to ask this same question for several documents.  This index implementation is designed to supported this use case.

## Details

- This index implementation is NOT thread-safe.  It is expected that a single thread will invoke all methods, from NewMatcher() to Close().
- This index will ONLY ever contain 0 or 1 documents.  Subsequent calls to Update() overwrite the previous document, regardless of using unique identifiers.
- The Batch() method is unsupported, and always returns an error.
- The Reader returned is NOT isolated, and will always see the currently indexed document.
- Currently, the Document() method on a Reader is not supported (this could be added in the future)

## Approach

- Since the index only ever contains a single document, data sizes are small.
- Therefore, avoid heavy document analysis and complex data structures.
- After regular document analysis is complete, use this structure in place.
- Do not build more complicated structures like vellums or roaring bitmaps.
- If additional structure is needed, prefer arrays which have good cache locality, and can be reused.
- Avoid copying data, prefer sub-slicing, and brute-force processing over arrays.
- Cache reusable parts of the query, as we expect the same query to be run over multiple documents.

## License

Apache License Version 2.0
