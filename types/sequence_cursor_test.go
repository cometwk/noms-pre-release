package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func newTestSequenceCursor(items [][]int) *sequenceCursor {
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

	var cur *sequenceCursor
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
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
}

func TestCursorGetMaxNPrevItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{101}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{102}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(3))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(4))
	assert.Equal(1, cur.idx)
}

func TestCursorGetMaxNPrevItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(3))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{101}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(3))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{102}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(3))
	assert.Equal(3, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{103}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{102, 103}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{101, 102, 103}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{104}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{103, 104}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{102, 103, 104}, cur.maxNPrevItems(3))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{105}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{104, 105}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{103, 104, 105}, cur.maxNPrevItems(3))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{106}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{105, 106}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{104, 105, 106}, cur.maxNPrevItems(3))
	assert.Equal(3, cur.idx)

	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cur.maxNPrevItems(7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cur.maxNPrevItems(8))

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{107}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{106, 107}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNPrevItems(3))
	assert.Equal(4, cur.idx)

	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(8))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(9))
}

func TestCursorGetMaxNNextItemsWithEmptySequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{}})
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(1))
}

func TestCursorGetMaxNNextItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{100}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNNextItems(4))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{101}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNNextItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{102}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{102}, cur.maxNNextItems(2))
	assert.Equal(0, cur.idx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(2))
	assert.Equal(1, cur.idx)
}

func TestCursorGetMaxNNextItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{100, 101, 102, 103}, cur.maxNNextItems(4))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(5))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(6))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(8))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(9))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{101, 102, 103}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{101, 102, 103}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{101, 102, 103}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(4))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(5))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(6))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(7))
	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNNextItems(8))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{102, 103}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{102, 103}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cur.maxNNextItems(4))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cur.maxNNextItems(5))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cur.maxNNextItems(6))
	assert.Equal([]sequenceItem{102, 103, 104, 105, 106, 107}, cur.maxNNextItems(7))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{103}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cur.maxNNextItems(4))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cur.maxNNextItems(5))
	assert.Equal([]sequenceItem{103, 104, 105, 106, 107}, cur.maxNNextItems(6))
	assert.Equal(3, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cur.maxNNextItems(4))
	assert.Equal([]sequenceItem{104, 105, 106, 107}, cur.maxNNextItems(5))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNNextItems(3))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNNextItems(4))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{106, 107}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{106, 107}, cur.maxNNextItems(2))
	assert.Equal([]sequenceItem{106, 107}, cur.maxNNextItems(3))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{107}, cur.maxNNextItems(1))
	assert.Equal([]sequenceItem{107}, cur.maxNNextItems(2))
	assert.Equal(3, cur.idx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNNextItems(1))
	assert.Equal(4, cur.idx)
}
