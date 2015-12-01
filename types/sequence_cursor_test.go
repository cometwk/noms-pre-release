package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func newTestSequenceCursor(items [][]int) *sequenceChunkerCursor {
	parent := newSequenceChunkerCursor(nil, items, 0, len(items), func(item sequenceCursorItem, idx int) sequenceItem {
		return item.([][]int)[idx] // item should be == items
	}, func(item sequenceItem) (sequenceCursorItem, int) {
		panic("not reachable")
	})
	return newSequenceChunkerCursor(parent, items[0], 0, len(items[0]), func(item sequenceCursorItem, idx int) sequenceItem {
		return item.([]int)[idx]
	}, func(item sequenceItem) (sequenceCursorItem, int) {
		return item, len(item.([]int))
	}).(*sequenceChunkerCursor)
}

// This also serves as a test for sequenceChunkerCursor.
// TODO in fact we should move this to a separate test_sequence_cursor.go file and test it separately in a "sequence_chunker_cursor.go" test.
func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	data := [][]int{[]int{100, 101}, []int{102}}

	cur := newTestSequenceCursor(data)

	assert.Equal(0, cur.indexInChunk())
	val, ok := cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(100), val)

	assert.False(cur.retreat())
	val, ok = cur.current()
	assert.False(ok)
	assert.Equal(nil, val)

	cur = newTestSequenceCursor(data)
	assert.True(cur.advance())

	assert.Equal(1, cur.indexInChunk())
	val, ok = cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(101), val)

	assert.True(cur.retreat())
	assert.Equal(0, cur.indexInChunk())
	val, ok = cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(100), val)

	assert.False(cur.retreat())
	val, ok = cur.current()
	assert.False(ok)
	assert.Equal(nil, val)

	cur = newTestSequenceCursor(data)
	assert.True(cur.advance())
	assert.True(cur.advance())

	assert.Equal(0, cur.indexInChunk())
	val, ok = cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(102), val)

	assert.False(cur.advance())
	val, ok = cur.current()
	assert.False(ok)
	assert.Equal(nil, val)
}

func TestCursorGetMaxNPrevItems(t *testing.T) {
	assert := assert.New(t)
	// TODO test an empty sequence?
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103, 104},
		[]int{105, 106, 107, 108, 109}})

	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{100}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{100}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{100}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{101}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{102}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(3, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{103}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{102, 103}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{101, 102, 103}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(4, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{104}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{103, 104}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{102, 103, 104}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{105}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{104, 105}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{106}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{105, 106}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{104, 105, 106}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{107}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{106, 107}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(3, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{108}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{107, 108}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{106, 107, 108}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(4, cur.leafIdx)

	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107, 108}, cursorGetMaxNPrevItems(cur, 9))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107, 108}, cursorGetMaxNPrevItems(cur, 10))

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{109}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{108, 109}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{107, 108, 109}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(5, cur.leafIdx)

	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107, 108, 109}, cursorGetMaxNPrevItems(cur, 9))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}, cursorGetMaxNPrevItems(cur, 10))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}, cursorGetMaxNPrevItems(cur, 11))
}

func TestCursorGetMaxNNextItems(t *testing.T) {
	assert := assert.New(t)
	// TODO test an empty sequence?
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{100}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{101}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(0, cur.leafIdx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(1, cur.leafIdx)
}

/*
func TestCursorGetMaxNNextItemsWithChunkSize(t *testing.T) {
	assert := assert.New(t)
	// TODO test an empty sequence?
	cur := newTestSequenceCursor(100, 5, 3)

	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(3, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(4, cur.leafIdx)
	assert.Equal([]sequenceItem{104}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(4, cur.leafIdx)
	assert.Equal([]sequenceItem{104}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(4, cur.leafIdx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(5, cur.leafIdx)
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(5, cur.leafIdx)

	cur = newTestSequenceCursor(100, 6, 3)

	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal(0, cur.leafIdx)
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 7))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(1, cur.leafIdx)
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(2, cur.leafIdx)
	assert.Equal([]sequenceItem{102, 103, 104, 105}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(3, cur.leafIdx)
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(3, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(4, cur.leafIdx)
	assert.Equal([]sequenceItem{104, 105}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(4, cur.leafIdx)
	assert.Equal([]sequenceItem{104, 105}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(4, cur.leafIdx)
	assert.Equal([]sequenceItem{104, 105}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(4, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(5, cur.leafIdx)
	assert.Equal([]sequenceItem{105}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(5, cur.leafIdx)
	assert.Equal([]sequenceItem{105}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(5, cur.leafIdx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(6, cur.leafIdx)
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(6, cur.leafIdx)
}
*/
