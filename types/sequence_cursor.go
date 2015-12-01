package types

import "github.com/attic-labs/noms/d"

// TODO no need for this to be an interface anymore, sequence_chunker_cursor can be moved in here, and the tests moved to sequence_cursor_test. The helper functions which construct these can be moved to where they're needed (e.g. meta_sequence).
type sequenceCursor interface {
	getParent() sequenceCursor
	clone() sequenceCursor
	current() (sequenceItem, bool)
	prevInChunk() (sequenceItem, bool)
	advance() bool
	retreat() bool
	indexInChunk() int
	seek(sequenceChunkerSeekFn, seekParentItemFn, sequenceCursorItem) sequenceCursorItem
}

// TODO is there actually any difference between sequenceItem and sequenceCursorItem?
type sequenceCursorItem interface{}

type sequenceChunkerSeekFn func(v, parent sequenceCursorItem) bool

// TODO parent/prev need documenting, and besides, they shouldn't all be sequenceCursorItems since it's really just an arbitrary "reduce" value.
type seekParentItemFn func(parent, prev, curr sequenceCursorItem) sequenceCursorItem

// Returns a slice of the previous |n| items in |seq|, excluding the current item in |seq|. Does not modify |seq|.
func cursorGetMaxNPrevItems(seq sequenceCursor, n int) []sequenceItem {
	prev := []sequenceItem{}

	retreater := seq.clone()
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

// Returns a slice of the next |n| items in |seq|, including the current item in |seq|. Does not
// TODO "plus last chunk"
// TODO this would be more useful if it returned a [][]sequenceItem, a list of items in each chunk.
func cursorGetMaxNNextItems(seq sequenceCursor, n int) []sequenceItem {
	next := []sequenceItem{}
	if n == 0 {
		return next
	}

	{
		current, ok := seq.current()
		if !ok {
			return next
		}
		next = append(next, current)
	}

	advancer := seq.clone()
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
