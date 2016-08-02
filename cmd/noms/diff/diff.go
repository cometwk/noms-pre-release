// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"fmt"
	"io"
	"os"

	"github.com/attic-labs/noms/go/types"
	"github.com/dustin/go-humanize"
)

var stopN int

type (
	diffFunc  func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{})
	lineFunc  func(w io.Writer, op prefixOp, key, val types.Value) error
	valueFunc func(k types.Value) types.Value
)

func shouldDescend(v1, v2 types.Value) bool {
	kind := v1.Type().Kind()
	return !types.IsPrimitiveKind(kind) && kind == v2.Type().Kind() && kind != types.RefKind
}

func Diff(w io.Writer, v1, v2 types.Value) error {
	return diff(w, types.NewPath(), nil, v1, v2)
}

func diff(w io.Writer, p types.Path, key, v1, v2 types.Value) error {
	if v1.Equals(v2) {
		return nil
	}

	if shouldDescend(v1, v2) {
		switch v1.Type().Kind() {
		case types.ListKind:
			return diffLists(w, p, v1.(types.List), v2.(types.List))
		case types.MapKind:
			return diffMaps(w, p, v1.(types.Map), v2.(types.Map))
		case types.SetKind:
			return diffSets(w, p, v1.(types.Set), v2.(types.Set))
		case types.StructKind:
			return diffStructs(w, p, v1.(types.Struct), v2.(types.Struct))
		default:
			panic("Unrecognized type in diff function")
		}
	}

	if err := line(w, DEL, key, v1); err != nil {
		return err
	}
	return line(w, ADD, key, v2)
}

func diffLists(w io.Writer, p types.Path, v1, v2 types.List) (err error) {
	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		v2.Diff(v1, spliceChan, stopChan)
		close(spliceChan)
	}()

	wroteHeader := false

	for splice := range spliceChan {
		if err != nil {
			break
		}

		if splice.SpRemoved == splice.SpAdded {
			// Heuristic: list only has modifications.
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl := v1.Get(splice.SpAt + i)
				newEl := v2.Get(splice.SpFrom + i)
				if shouldDescend(lastEl, newEl) {
					// TODO: writeFooter?
					idx := types.Number(splice.SpAt + i)
					err = diff(w, p.AddIndex(idx), idx, lastEl, newEl)
				} else {
					wroteHeader, err = writeHeader(w, p, wroteHeader)
					if err == nil {
						err = line(w, DEL, nil, v1.Get(splice.SpAt+i))
					}
					if err == nil {
						err = line(w, ADD, nil, v2.Get(splice.SpFrom+i))
					}
				}
			}
			continue
		}

		// Heuristic: list only has additions/removals.
		for i := uint64(0); i < splice.SpRemoved && err == nil; i++ {
			wroteHeader, err = writeHeader(w, p, wroteHeader)
			if err == nil {
				err = line(w, DEL, nil, v1.Get(splice.SpAt+i))
			}
		}
		for i := uint64(0); i < splice.SpAdded && err == nil; i++ {
			wroteHeader, err = writeHeader(w, p, wroteHeader)
			if err == nil {
				err = line(w, ADD, nil, v2.Get(splice.SpFrom+i))
			}
		}
	}

	if err == nil {
		_, err = writeFooter(w, wroteHeader)
	}

	if err != nil {
		stopChan <- struct{}{}
		// Wait for diff to stop.
		fmt.Fprintln(os.Stderr, "waiting for diff to stop (lists)")
		for range spliceChan {
		}
		fmt.Fprintln(os.Stderr, " ... it stopped (lists)")
	}
	return
}

func diffMaps(w io.Writer, p types.Path, v1, v2 types.Map) error {
	return diffOrdered(w, p, line, func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
		v2.Diff(v1, cc, sc)
	},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(k) },
		func(k types.Value) types.Value { return v2.Get(k) },
	)
}

func diffStructs(w io.Writer, p types.Path, v1, v2 types.Struct) error {
	return diffOrdered(w, p, field, func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
		v2.Diff(v1, cc, sc)
	},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(string(k.(types.String))) },
		func(k types.Value) types.Value { return v2.Get(string(k.(types.String))) },
	)
}

func diffSets(w io.Writer, p types.Path, v1, v2 types.Set) error {
	return diffOrdered(w, p, line, func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
		v2.Diff(v1, cc, sc)
	},
		func(k types.Value) types.Value { return nil },
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return k },
	)
}

func diffOrdered(w io.Writer, p types.Path, lf lineFunc, df diffFunc, kf, v1, v2 valueFunc) (err error) {
	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		df(changeChan, stopChan)
		close(changeChan)
	}()

	wroteHeader := false

	for change := range changeChan {
		if err != nil {
			break
		}

		k := kf(change.V)

		switch change.ChangeType {
		case types.DiffChangeAdded:
			wroteHeader, err = writeHeader(w, p, wroteHeader)
			if err == nil {
				err = lf(w, ADD, k, v2(change.V))
			}
		case types.DiffChangeRemoved:
			wroteHeader, err = writeHeader(w, p, wroteHeader)
			if err == nil {
				err = lf(w, DEL, k, v1(change.V))
			}
		case types.DiffChangeModified:
			c1, c2 := v1(change.V), v2(change.V)
			if shouldDescend(c1, c2) {
				wroteHeader, err = writeFooter(w, wroteHeader)
				if err == nil {
					err = diff(w, p.AddIndex(k), change.V, c1, c2)
				}
			} else {
				wroteHeader, err = writeHeader(w, p, wroteHeader)
				if err == nil {
					err = lf(w, DEL, k, c1)
				}
				if err == nil {
					err = lf(w, ADD, k, c2)
				}
			}
		default:
			panic("unknown change type")
		}
	}

	if err == nil {
		_, err = writeFooter(w, wroteHeader)
	}

	if err != nil {
		stopChan <- struct{}{}
		// Wait for diff to stop.
		x := stopN
		stopN++
		fmt.Fprintln(os.Stderr, "waiting for diff to stop", x)
		for range changeChan {
		}
		fmt.Fprintln(os.Stderr, " ... it stopped", x)
	}
	return
}

func writeHeader(w io.Writer, p types.Path, wroteHeader bool) (bool, error) {
	if wroteHeader {
		return true, nil
	}

	var err error
	if len(p) == 0 {
		_, err = w.Write([]byte("(root)"))
	} else {
		_, err = w.Write([]byte(p.String()))
	}
	if err == nil {
		_, err = w.Write([]byte(" {\n"))
	}
	return true, err
}

func writeFooter(w io.Writer, wroteHeader bool) (bool, error) {
	if !wroteHeader {
		return false, nil
	}

	_, err := w.Write([]byte("  }\n"))
	return false, err
}

func line(w io.Writer, op prefixOp, key, val types.Value) error {
	pw := newPrefixWriter(w, op)
	if key != nil {
		if err := writeEncodedValue(pw, key); err != nil {
			return err
		}
		if _, err := w.Write([]byte(": ")); err != nil {
			return err
		}
	}
	if err := writeEncodedValue(pw, val); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n"))
	return err
}

func field(w io.Writer, op prefixOp, name, val types.Value) error {
	pw := newPrefixWriter(w, op)
	if _, err := pw.Write([]byte(name.(types.String))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(": ")); err != nil {
		return err
	}
	if err := types.WriteEncodedValue(pw, val); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n"))
	return err
}

func writeEncodedValue(w io.Writer, v types.Value) error {
	if v.Type().Kind() != types.BlobKind {
		return types.WriteEncodedValue(w, v)
	}

	if _, err := w.Write([]byte("Blob (")); err != nil {
		return err
	}
	if _, err := w.Write([]byte(humanize.Bytes(v.(types.Blob).Len()))); err != nil {
		return err
	}
	_, err := w.Write([]byte(")"))
	return err
}
