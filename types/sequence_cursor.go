package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
)

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent      *sequenceCursor
	item        sequenceItem
	idx, length int
	getItem     getItemFn
	readChunk   readChunkFn
}

// getItemFn takes a parent in the sequence and an index into that parent, and returns the child item, equivalent to `child = parent[idx]`. The parent and the child aren't necessarily the same type.
type getItemFn func(parent sequenceItem, idx int) (child sequenceItem)

// readChunkFn takes an item in the sequence which references another sequence of items, and returns that sequence along with its length.
type readChunkFn func(reference sequenceItem) (sequence sequenceItem, length int)

func newSequenceCursor(parent *sequenceCursor, item sequenceItem, idx, length int, getItem getItemFn, readChunk readChunkFn) *sequenceCursor {
	return &sequenceCursor{parent, item, idx, length, getItem, readChunk}
}

// Returns the value the cursor refers to. Fails an assertion if the cursor doesn't point to a value.
func (cur *sequenceCursor) current() sequenceItem {
	item, ok := cur.maybeCurrent()
	d.Chk.True(ok)
	return item
}

// Returns the value the cursor refers to, if any. If the cursor doesn't point to a value, returns (nil, false).
func (cur *sequenceCursor) maybeCurrent() (sequenceItem, bool) {
	switch {
	case cur.idx < -1 || cur.idx > cur.length:
		panic("illegal")
	case cur.idx == -1 || cur.idx == cur.length:
		return nil, false
	default:
		return cur.getItem(cur.item, cur.idx), true
	}
}

func (cur *sequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *sequenceCursor) advance() bool {
	if cur.idx < cur.length {
		cur.idx++
		if cur.parent != nil && cur.idx == 0 {
			// This advance caused this cursor to step from an invalid state before the start, to a valid state exactly at the start. The parent must be immediately advanced to compensate, because it will point to before the start.
			cur.parent.advance()
		}
		if cur.idx < cur.length {
			return true
		}
	}
	if cur.parent != nil && cur.parent.advance() {
		cur.item, cur.length = cur.readChunk(cur.parent.current())
		cur.idx = 0
		return true
	}
	return false
}

func (cur *sequenceCursor) retreat() bool {
	if cur.idx >= 0 {
		cur.idx--
		if cur.parent != nil && cur.idx == cur.length-1 {
			// This retreat caused this cursor to step from an invalid state past the end, to a valid state exactly at the end. The parent must be immediately retreated to compensate, because it will point past the end.
			cur.parent.retreat()
		}
		if cur.idx >= 0 {
			return true
		}
	}
	if cur.parent != nil && cur.parent.retreat() {
		cur.item, cur.length = cur.readChunk(cur.parent.current())
		cur.idx = cur.length - 1
		return true
	}
	return false
}

func (cur *sequenceCursor) clone() *sequenceCursor {
	var parent *sequenceCursor
	if cur.parent != nil {
		parent = cur.parent.clone()
	}
	return &sequenceCursor{parent, cur.item, cur.idx, cur.length, cur.getItem, cur.readChunk}
}

type sequenceCursorSeekCompareFn func(carry interface{}, item sequenceItem) bool

type sequenceCursorSeekStepFn func(carry interface{}, prev, current sequenceItem) interface{}

// Seeks the cursor to the first position in the sequence where |compare| returns true. During seeking, the caller can build up an arbitrary carry value, passed to |compare| and |step|. The carry value is initialized as |carry|, but will be replaced with the return value of |step|.
func (cur *sequenceCursor) seek(compare sequenceCursorSeekCompareFn, step sequenceCursorSeekStepFn, carry interface{}) interface{} {
	d.Chk.NotNil(compare)
	d.Chk.NotNil(step)

	if cur.parent != nil {
		carry = cur.parent.seek(compare, step, carry)
		cur.item, cur.length = cur.readChunk(cur.parent.current())
	}

	cur.idx = sort.Search(cur.length, func(i int) bool {
		return compare(carry, cur.getItem(cur.item, i))
	})

	if cur.idx == cur.length {
		cur.idx = cur.length - 1
	}

	var prev sequenceItem
	if cur.idx > 0 {
		prev = cur.getItem(cur.item, cur.idx-1)
	}

	return step(carry, prev, cur.getItem(cur.item, cur.idx))
}

// TODO: This comment is wrong.
// Returns a slice of the previous |n| items in |cur|, excluding the current item in |cur|. Does not modify |cur|.
func (cur *sequenceCursor) maxNPrevItems(n int) (sequenceItem, [][]sequenceItem) {
	chunks := [][]sequenceItem{}
	working := []sequenceItem{}

	liftWorking := func() {
		if len(working) > 0 {
			for i := 0; i < len(working)/2; i++ {
				t := working[i]
				working[i] = working[len(working)-i-1]
				working[len(working)-i-1] = t
			}
			chunks = append(chunks, working)
			working = []sequenceItem{}
		}
	}

	retreater := cur.clone()
	for i := 0; i < n && retreater.retreat(); i++ {
		working = append(working, retreater.current())
		if retreater.indexInChunk() == 0 {
			liftWorking()
		}
	}

	var prevInChunk sequenceItem
	if retreater.indexInChunk() > 0 && retreater.retreat() {
		prevInChunk = retreater.current()
	}

	liftWorking()

	for i := 0; i < len(chunks)/2; i++ {
		t := chunks[i]
		chunks[i] = chunks[len(chunks)-i-1]
		chunks[len(chunks)-i-1] = t
	}

	return prevInChunk, chunks
}

// TODO: This comment is wrong, and this comment/name should incorporate "with at least the last chunk".
// Returns a slice of the next |n| items in |cur|, including the current item in |cur|. Does not modify |cur|.
func (cur *sequenceCursor) maxNNextItems(n int) (sequenceItem, [][]sequenceItem) {
	chunks := [][]sequenceItem{}
	working := []sequenceItem{}

	// TODO go back to a prevInChunk() method? use that everywhere with nzeChunk, clearer than assuming nil, though I could assert that. Or maybe maxNNextItems should return the previous item in the chunk as well.
	var prevInChunk sequenceItem
	if cur.indexInChunk() > 0 {
		curToPrev := cur.clone()
		d.Chk.True(curToPrev.retreat())
		prevInChunk = curToPrev.current()
	}

	liftWorking := func() {
		if len(working) > 0 {
			chunks = append(chunks, working)
			working = []sequenceItem{}
		}
	}

	if current, ok := cur.maybeCurrent(); ok {
		working = append(working, current)
	} else {
		// TODO: What about the case that the cursor is at position -1? This will return an empty list, but arguably it should return the full first chunk. Unclear.
		return prevInChunk, chunks
	}

	advancer := cur.clone()
	for i := 1; i < n; i++ {
		if !advancer.advance() {
			liftWorking()
			return prevInChunk, chunks
		}
		if advancer.indexInChunk() == 0 {
			liftWorking()
		}
		working = append(working, advancer.current())
	}

	for advancer.advance() && advancer.indexInChunk() > 0 {
		working = append(working, advancer.current())
	}
	liftWorking()

	return prevInChunk, chunks
}
