package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func newTestSequenceCursor(items [][]int) *sequenceCursor {
	parent := newSequenceCursor(nil, items, 0, len(items), func(item sequenceItem, idx int) sequenceItem {
		return item.([][]int)[idx] // item should be == items
	}, func(item sequenceItem) (sequenceItem, int) {
		panic("not reachable")
	})
	return newSequenceCursor(parent, items[0], 0, len(items[0]), func(item sequenceItem, idx int) sequenceItem {
		return item.([]int)[idx]
	}, func(item sequenceItem) (sequenceItem, int) {
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
		val, ok := cur.maybeCurrent()
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
	prev, chunks := cur.maxNPrevItems(0)
	assert.Equal(nil, prev)
	assert.Equal([][]sequenceItem{}, chunks)
	prev, chunks = cur.maxNPrevItems(1)
	assert.Equal(nil, prev)
	assert.Equal([][]sequenceItem{}, chunks)
}

func TestCursorGetMaxNPrevItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assertMaxNPrevItems := func(expectPrev sequenceItem, expectChunks [][]sequenceItem, n int) {
		prev, chunks := cur.maxNPrevItems(n)
		assert.Equal(expectPrev, prev)
		assert.Equal(expectChunks, chunks)
	}

	assertMaxNPrevItems(nil, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 3)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 3)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{101}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}}, 3)
	assert.Equal(0, cur.idx)

	assert.False(cur.advance())
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{102}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{101}, []sequenceItem{102}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}, []sequenceItem{102}}, 3)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}, []sequenceItem{102}}, 4)
	assert.Equal(1, cur.idx)
}

func TestCursorGetMaxNPrevItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assertMaxNPrevItems := func(expectPrev sequenceItem, expectChunks [][]sequenceItem, n int) {
		prev, chunks := cur.maxNPrevItems(n)
		assert.Equal(expectPrev, prev)
		assert.Equal(expectChunks, chunks)
	}

	assertMaxNPrevItems(nil, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 3)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(100, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 3)
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(101, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(100, [][]sequenceItem{[]sequenceItem{101}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101}}, 3)
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(102, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(101, [][]sequenceItem{[]sequenceItem{102}}, 1)
	assertMaxNPrevItems(100, [][]sequenceItem{[]sequenceItem{101, 102}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102}}, 3)
	assert.Equal(3, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(nil, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(102, [][]sequenceItem{[]sequenceItem{103}}, 1)
	assertMaxNPrevItems(101, [][]sequenceItem{[]sequenceItem{102, 103}}, 2)
	assertMaxNPrevItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}}, 3)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(104, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{104}}, 1)
	assertMaxNPrevItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104}}, 2)
	assertMaxNPrevItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104}}, 3)
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(105, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(104, [][]sequenceItem{[]sequenceItem{105}}, 1)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{104, 105}}, 2)
	assertMaxNPrevItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105}}, 3)
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assertMaxNPrevItems(106, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(105, [][]sequenceItem{[]sequenceItem{106}}, 1)
	assertMaxNPrevItems(104, [][]sequenceItem{[]sequenceItem{105, 106}}, 2)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106}}, 3)
	assert.Equal(3, cur.idx)

	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106}}, 7)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106}}, 8)

	assert.False(cur.advance())
	assertMaxNPrevItems(107, [][]sequenceItem{}, 0)
	assertMaxNPrevItems(106, [][]sequenceItem{[]sequenceItem{107}}, 1)
	assertMaxNPrevItems(105, [][]sequenceItem{[]sequenceItem{106, 107}}, 2)
	assertMaxNPrevItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 3)
	assert.Equal(4, cur.idx)

	assertMaxNPrevItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 7)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 8)
	assertMaxNPrevItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 9)
}

func TestCursorGetMaxNNextItemsWithEmptySequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{}})
	prev, chunks := cur.maxNPrevItems(0)
	assert.Equal(nil, prev)
	assert.Equal([][]sequenceItem{}, chunks)
	prev, chunks = cur.maxNPrevItems(1)
	assert.Equal(nil, prev)
	assert.Equal([][]sequenceItem{}, chunks)
}

func TestCursorGetMaxNNextItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assertMaxNNextItems := func(expectPrev sequenceItem, expectChunks [][]sequenceItem, n int) {
		prev, chunks := cur.maxNNextItems(n)
		assert.Equal(expectPrev, prev)
		assert.Equal(expectChunks, chunks)
	}

	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 0)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100}}, 1)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}}, 2)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}, []sequenceItem{102}}, 3)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100}, []sequenceItem{101}, []sequenceItem{102}}, 4)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{101}}, 0)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{101}}, 1)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{101}, []sequenceItem{102}}, 2)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{101}, []sequenceItem{102}}, 3)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{102}}, 0)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{102}}, 1)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{102}}, 2)
	assert.Equal(0, cur.idx)

	assert.False(cur.advance())
	assertMaxNNextItems(102, [][]sequenceItem{}, 0)
	assertMaxNNextItems(102, [][]sequenceItem{}, 1)
	assertMaxNNextItems(102, [][]sequenceItem{}, 2)
	assert.Equal(1, cur.idx)
}

func TestCursorGetMaxNNextItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assertMaxNNextItems := func(expectPrev sequenceItem, expectChunks [][]sequenceItem, n int) {
		prev, chunks := cur.maxNNextItems(n)
		assert.Equal(expectPrev, prev)
		assert.Equal(expectChunks, chunks)
	}

	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}}, 0)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}}, 1)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}}, 2)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}}, 3)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}}, 4)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 5)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 6)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 7)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 8)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{100, 101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 9)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}}, 0)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}}, 1)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}}, 2)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}}, 3)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 4)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 5)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 6)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 7)
	assertMaxNNextItems(100, [][]sequenceItem{[]sequenceItem{101, 102, 103}, []sequenceItem{104, 105, 106, 107}}, 8)
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}}, 0)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}}, 1)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}}, 2)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104, 105, 106, 107}}, 3)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104, 105, 106, 107}}, 4)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104, 105, 106, 107}}, 5)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104, 105, 106, 107}}, 6)
	assertMaxNNextItems(101, [][]sequenceItem{[]sequenceItem{102, 103}, []sequenceItem{104, 105, 106, 107}}, 7)
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}}, 0)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}}, 1)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105, 106, 107}}, 2)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105, 106, 107}}, 3)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105, 106, 107}}, 4)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105, 106, 107}}, 5)
	assertMaxNNextItems(102, [][]sequenceItem{[]sequenceItem{103}, []sequenceItem{104, 105, 106, 107}}, 6)
	assert.Equal(3, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 0)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 1)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 2)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 3)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 4)
	assertMaxNNextItems(nil, [][]sequenceItem{[]sequenceItem{104, 105, 106, 107}}, 5)
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 0)
	assertMaxNNextItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 1)
	assertMaxNNextItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 2)
	assertMaxNNextItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 3)
	assertMaxNNextItems(104, [][]sequenceItem{[]sequenceItem{105, 106, 107}}, 4)
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(105, [][]sequenceItem{[]sequenceItem{106, 107}}, 0)
	assertMaxNNextItems(105, [][]sequenceItem{[]sequenceItem{106, 107}}, 1)
	assertMaxNNextItems(105, [][]sequenceItem{[]sequenceItem{106, 107}}, 2)
	assertMaxNNextItems(105, [][]sequenceItem{[]sequenceItem{106, 107}}, 3)
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assertMaxNNextItems(106, [][]sequenceItem{[]sequenceItem{107}}, 0)
	assertMaxNNextItems(106, [][]sequenceItem{[]sequenceItem{107}}, 1)
	assertMaxNNextItems(106, [][]sequenceItem{[]sequenceItem{107}}, 2)
	assert.Equal(3, cur.idx)

	assert.False(cur.advance())
	assertMaxNNextItems(107, [][]sequenceItem{}, 0)
	assertMaxNNextItems(107, [][]sequenceItem{}, 1)
	assert.Equal(4, cur.idx)
}

func TestCursorSeek(t *testing.T) {
	assert := assert.New(t)
	var cur *sequenceCursor

	reset := func() {
		cur = newTestSequenceCursor([][]int{
			[]int{100, 101, 102, 103},
			[]int{104, 105, 106, 107},
		})
	}

	assertSeeksTo := func(expected sequenceItem, seekTo int) {
		// The value being carried around here is the level of the tree being seeked in. The seek is initialized with 0, so carry value passed to the comparison function on the first level should be 0. Subsequent steps increment this number, so 1 should be passed into the comparison function for the second level. When the seek exits, the final step should increment it again, so the result should be 2.
		result := cur.seek(func(carry interface{}, val sequenceItem) bool {
			switch val := val.(type) {
			case []int:
				assert.Equal(0, carry)
				return val[len(val)-1] >= seekTo
			case int:
				assert.Equal(1, carry)
				return val >= seekTo
			default:
				panic("illegal")
			}
		}, func(carry interface{}, prev, current sequenceItem) interface{} {
			switch current.(type) {
			case []int:
				assert.Equal(0, carry)
			case int:
				assert.Equal(1, carry)
			}
			return carry.(int) + 1
		}, 0)
		assert.Equal(2, result)
		assert.Equal(expected, cur.current())
	}

	// Test seeking immediately to values on cursor construction.
	reset()
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		reset()
		assertSeeksTo(sequenceItem(i), i)
	}
	reset()
	assertSeeksTo(sequenceItem(107), 108)

	// Test reusing an existing cursor to seek all over the place.
	reset()
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		assertSeeksTo(sequenceItem(i), i)
	}
	assertSeeksTo(sequenceItem(107), 108)
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		assertSeeksTo(sequenceItem(i), i)
	}
	assertSeeksTo(sequenceItem(107), 108)
}
