package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
)

// sequenceCursor wraps a *sequenceCursor to give it the ability to advance/retreat through individual items.
type sequenceCursor struct {
	parent      *sequenceCursor
	item        sequenceCursorItem
	idx, length int
	getItem     getItemFn
	readChunk   readChunkFn
}

// TODO is there actually any difference between sequenceItem and sequenceCursorItem?
type sequenceCursorItem interface{}

type sequenceChunkerSeekFn func(v, parent sequenceCursorItem) bool

// TODO parent/prev need documenting, and besides, they shouldn't all be sequenceCursorItems since it's really just an arbitrary "reduce" value.
type seekParentItemFn func(parent, prev, curr sequenceCursorItem) sequenceCursorItem

// TODO comment
type getItemFn func(sequenceCursorItem, int) sequenceItem

// readChunkFn takes an item in the sequence which points to a chunk, and returns the sequence of items in that chunk along with its length.
type readChunkFn func(sequenceItem) (sequenceCursorItem, int)

func newSequenceChunkerCursor(parent *sequenceCursor, item sequenceCursorItem, idx, length int, getItem getItemFn, readChunk readChunkFn) *sequenceCursor {
	return &sequenceCursor{parent, item, idx, length, getItem, readChunk}
}

func (cur *sequenceCursor) current() (sequenceItem, bool) {
	switch {
	case cur.idx < -1 || cur.idx > cur.length:
		panic("illegal")
	case cur.idx == -1 || cur.idx == cur.length:
		return nil, false
	default:
		return cur.getItem(cur.item, cur.idx), true
	}
}

func (cur *sequenceCursor) prevInChunk() (sequenceItem, bool) {
	if cur.idx > 0 {
		return cur.getItem(cur.item, cur.idx-1), true
	} else {
		return nil, false
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
		current, ok := cur.parent.current()
		d.Chk.True(ok)
		cur.item, cur.length = cur.readChunk(current)
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
		current, ok := cur.parent.current()
		d.Chk.True(ok)
		cur.item, cur.length = cur.readChunk(current)
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

// XXX make this return a copy, not the actual parent, because giving direct access to the parent - and letting callers mutate it - can cause subtle bugs.
func (cur *sequenceCursor) getParent() *sequenceCursor {
	if cur.parent == nil {
		return nil
	}
	return cur.parent
}

// TODO this needs testing.
func (cur *sequenceCursor) seek(seekFn sequenceChunkerSeekFn, parentItemFn seekParentItemFn, parentItem sequenceCursorItem) sequenceCursorItem {
	d.Chk.NotNil(seekFn)
	d.Chk.NotNil(parentItemFn)

	if cur.parent != nil {
		parentItem = cur.parent.seek(seekFn, parentItemFn, parentItem)
		current, ok := cur.parent.current()
		d.Chk.True(ok)
		cur.item, cur.length = cur.readChunk(current)
	}

	cur.idx = sort.Search(cur.length, func(i int) bool {
		return seekFn(cur.getItem(cur.item, i), parentItem)
	})

	if cur.idx == cur.length {
		cur.idx = cur.length - 1
	}

	var prev sequenceCursorItem
	if cur.idx > 0 {
		prev = cur.getItem(cur.item, cur.idx-1)
	}

	return parentItemFn(parentItem, prev, cur.getItem(cur.item, cur.idx))
}

// Returns a slice of the previous |n| items in |cur|, excluding the current item in |cur|. Does not modify |cur|.
func (cur *sequenceCursor) maxNPrevItems(n int) []sequenceItem {
	prev := []sequenceItem{}

	retreater := cur.clone()
	for i := 0; i < n && retreater.retreat(); i++ {
		current, ok := retreater.current()
		d.Chk.True(ok)
		prev = append(prev, current)
	}

	for i := 0; i < len(prev)/2; i++ {
		t := prev[i]
		prev[i] = prev[len(prev)-i-1]
		prev[len(prev)-i-1] = t
	}

	return prev
}

// Returns a slice of the next |n| items in |cur|, including the current item in |cur|. Does not modify |cur|.
// TODO "plus last chunk"
// TODO this would be more useful if it returned a [][]sequenceItem, a list of items in each chunk.
func (cur *sequenceCursor) maxNNextItems(n int) []sequenceItem {
	next := []sequenceItem{}
	if n == 0 {
		return next
	}

	{
		current, ok := cur.current()
		if !ok {
			return next
		}
		next = append(next, current)
	}

	advancer := cur.clone()
	for i := 1; i < n; i++ {
		if !advancer.advance() {
			return next
		}
		current, ok := advancer.current()
		d.Chk.True(ok)
		next = append(next, current)
	}

	for advancer.advance() && advancer.indexInChunk() > 0 {
		current, ok := advancer.current()
		d.Chk.True(ok)
		next = append(next, current)
	}

	return next
}
