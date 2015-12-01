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
	})
}

func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	var cur sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([][]int{[]int{100, 101}, []int{102}})
	}
	expect := func(expectIdx int, expectOk bool, expectVal sequenceItem) {
		assert.Equal(expectIdx, cur.indexInChunk())
		val, ok := cur.current()
		assert.Equal(expectOk, ok)
		assert.Equal(expectVal, val)
	}

	reset()
	expect(0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, false, nil)
	assert.False(cur.retreat())
	expect(-1, false, nil)

	reset()
	assert.True(cur.advance())
	expect(1, true, sequenceItem(101))
	assert.True(cur.retreat())
	expect(0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, false, nil)
	assert.False(cur.retreat())
	expect(-1, false, nil)

	reset()
	assert.True(cur.advance())
	expect(1, true, sequenceItem(101))
	assert.True(cur.advance())
	expect(0, true, sequenceItem(102))
	assert.False(cur.advance())
	expect(1, false, nil)
	assert.False(cur.advance())
	expect(1, false, nil)
	assert.True(cur.retreat())
	expect(0, true, sequenceItem(102))
	assert.True(cur.retreat())
	expect(1, true, sequenceItem(101))
	assert.True(cur.retreat())
	expect(0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, false, nil)
}

func TestCursorGetMaxNPrevItemsWithEmptySequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{}})
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 1))
}

func TestCursorGetMaxNPrevItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

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
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{101}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(0, cur.leafIdx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{102}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNPrevItems(cur, 4))
	assert.Equal(1, cur.leafIdx)
}

func TestCursorGetMaxNPrevItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

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
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{104}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{103, 104}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{102, 103, 104}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{105}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{104, 105}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{103, 104, 105}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{106}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{105, 106}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{104, 105, 106}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(3, cur.leafIdx)

	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cursorGetMaxNPrevItems(cur, 7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cursorGetMaxNPrevItems(cur, 8))

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal([]sequenceItem{107}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal([]sequenceItem{106, 107}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(4, cur.leafIdx)

	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNPrevItems(cur, 7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNPrevItems(cur, 8))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNPrevItems(cur, 9))
}

func TestCursorGetMaxNNextItemsWithEmptySequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{}})
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
}

func TestCursorGetMaxNNextItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
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

func TestCursorGetMaxNNextItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 8))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 9))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{101, 102, 103}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{101, 102, 103}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{101, 102, 103}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 7))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 8))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{102, 103}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{102, 103}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 7))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{103}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 6))
	assert.Equal(3, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cursorGetMaxNNextItems(cur, 5))
	assert.Equal(0, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal([]sequenceItem{105, 106, 107}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(1, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{106, 107}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{106, 107}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal([]sequenceItem{106, 107}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(2, cur.leafIdx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{107}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal([]sequenceItem{107}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(3, cur.leafIdx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(4, cur.leafIdx)
}
