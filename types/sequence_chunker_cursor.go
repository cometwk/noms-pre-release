package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
)

// sequenceChunkerCursor wraps a sequenceCursor to give it the ability to advance/retreat through individual items.
type sequenceChunkerCursor struct {
	// TODO names like "leaf" and so on will need to change.
	parent           sequenceCursor
	leaf             sequenceCursorItem
	leafIdx, leafLen int
	getItem          getItemFn
	readChunk        readChunkFn
}

// TODO comment
type getItemFn func(sequenceCursorItem, int) sequenceItem

// readChunkFn takes an item in the sequence which points to a chunk, and returns the sequence of items in that chunk along with its length.
type readChunkFn func(sequenceItem) (sequenceCursorItem, int)

func newSequenceChunkerCursor(parent sequenceCursor, leaf sequenceCursorItem, leafIdx, leafLen int, getItem getItemFn, readChunk readChunkFn) sequenceCursor {
	return &sequenceChunkerCursor{parent, leaf, leafIdx, leafLen, getItem, readChunk}
}

func (scc *sequenceChunkerCursor) current() (sequenceItem, bool) {
	switch {
	case scc.leafIdx < -1 || scc.leafIdx > scc.leafLen:
		panic("illegal")
	case scc.leafIdx == -1 || scc.leafIdx == scc.leafLen:
		return nil, false
	default:
		return scc.getItem(scc.leaf, scc.leafIdx), true
	}
}

func (scc *sequenceChunkerCursor) prevInChunk() (sequenceItem, bool) {
	if scc.leafIdx > 0 {
		return scc.getItem(scc.leaf, scc.leafIdx-1), true
	} else {
		return nil, false
	}
}

func (scc *sequenceChunkerCursor) indexInChunk() int {
	return scc.leafIdx
}

func (scc *sequenceChunkerCursor) advance() bool {
	if scc.leafIdx < scc.leafLen {
		scc.leafIdx++
		if scc.leafIdx < scc.leafLen {
			return true
		}
	}
	if scc.parent != nil && scc.parent.advance() {
		current, ok := scc.parent.current()
		d.Chk.True(ok)
		scc.leaf, scc.leafLen = scc.readChunk(current)
		scc.leafIdx = 0
		return true
	}
	return false
}

func (scc *sequenceChunkerCursor) retreat() bool {
	if scc.leafIdx >= 0 {
		scc.leafIdx--
		if scc.leafIdx >= 0 {
			return true
		}
	}
	if scc.parent != nil && scc.parent.retreat() {
		current, ok := scc.parent.current()
		d.Chk.True(ok)
		scc.leaf, scc.leafLen = scc.readChunk(current)
		scc.leafIdx = scc.leafLen - 1
		return true
	}
	return false
}

func (scc *sequenceChunkerCursor) clone() sequenceCursor {
	var parent sequenceCursor
	if scc.parent != nil {
		parent = scc.parent.clone()
	}
	return &sequenceChunkerCursor{parent, scc.leaf, scc.leafIdx, scc.leafLen, scc.getItem, scc.readChunk}
}

// XXX make this return a copy, not the actual parent, because giving direct access to the parent - and letting callers mutate it - can cause subtle bugs.
func (scc *sequenceChunkerCursor) getParent() sequenceCursor {
	if scc.parent == nil {
		return nil
	}
	return scc.parent
}

// TODO this needs testing
func (scc *sequenceChunkerCursor) seek(seekFn sequenceChunkerSeekFn, parentItemFn seekParentItemFn, parentItem sequenceCursorItem) sequenceCursorItem {
	d.Chk.NotNil(seekFn)
	d.Chk.NotNil(parentItemFn)

	if scc.parent != nil {
		parentItem = scc.parent.seek(seekFn, parentItemFn, parentItem)
		cur, ok := scc.parent.current()
		d.Chk.True(ok)
		scc.leaf, scc.leafLen = scc.readChunk(cur)
	}

	scc.leafIdx = sort.Search(scc.leafLen, func(i int) bool {
		return seekFn(scc.getItem(scc.leaf, i), parentItem)
	})

	if scc.leafIdx == scc.leafLen {
		scc.leafIdx = scc.leafLen - 1
	}

	var prev sequenceCursorItem
	if scc.leafIdx > 0 {
		prev = scc.getItem(scc.leaf, scc.leafIdx-1)
	}

	return parentItemFn(parentItem, prev, scc.getItem(scc.leaf, scc.leafIdx))
}
