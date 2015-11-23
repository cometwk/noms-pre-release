package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

type testSequenceCursor struct {
	idx, offset, size int
}

func newTestSequenceCursor(offset, size int) *testSequenceCursor {
	return &testSequenceCursor{0, offset, size}
}

func (cur *testSequenceCursor) getParent() sequenceCursor {
	return nil
}

func (cur *testSequenceCursor) clone() sequenceCursor {
	return &testSequenceCursor{cur.idx, cur.offset, cur.size}
}

func (cur *testSequenceCursor) current() (sequenceItem, bool) {
	if cur.idx < 0 || cur.idx == cur.size {
		return nil, false
	} else {
		return cur.idx + cur.offset, true
	}
}

func (cur *testSequenceCursor) advance() bool {
	if cur.idx < cur.size-1 {
		cur.idx++
		return true
	}
	if cur.idx == cur.size-1 {
		cur.idx++
	}
	return false
}

func (cur *testSequenceCursor) retreat() bool {
	if cur.idx > 0 {
		cur.idx--
		return true
	}
	if cur.idx == 0 {
		cur.idx--
	}
	return false
}

func (cur *testSequenceCursor) indexInChunk() int {
	return cur.idx
}

func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	cur := newTestSequenceCursor(100, 3)

	val, ok := cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(100), val)

	assert.False(cur.retreat())
	val, ok = cur.current()
	assert.False(ok)
	assert.Equal(nil, val)

	cur = newTestSequenceCursor(100, 3)
	assert.True(cur.advance())

	val, ok = cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(101), val)

	assert.True(cur.retreat())
	val, ok = cur.current()
	assert.True(ok)
	assert.Equal(sequenceItem(100), val)

	assert.False(cur.retreat())
	val, ok = cur.current()
	assert.False(ok)
	assert.Equal(nil, val)

	cur = newTestSequenceCursor(100, 3)
	assert.True(cur.advance())
	assert.True(cur.advance())

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
	cur := newTestSequenceCursor(100, 42)

	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal(0, cur.idx)
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal(1, cur.idx)
	assert.Equal([]sequenceItem{100}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal(2, cur.idx)
	assert.Equal([]sequenceItem{101}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal(2, cur.idx)
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNPrevItems(cur, 0))
	assert.Equal(3, cur.idx)
	assert.Equal([]sequenceItem{102}, cursorGetMaxNPrevItems(cur, 1))
	assert.Equal(3, cur.idx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNPrevItems(cur, 2))
	assert.Equal(3, cur.idx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNPrevItems(cur, 3))
	assert.Equal(3, cur.idx)
}

func TestCursorGetMaxNNextItems(t *testing.T) {
	assert := assert.New(t)
	// TODO test an empty sequence?
	cur := newTestSequenceCursor(100, 3)

	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(0, cur.idx)
	assert.Equal([]sequenceItem{100}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(0, cur.idx)
	assert.Equal([]sequenceItem{100, 101}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(0, cur.idx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(0, cur.idx)
	assert.Equal([]sequenceItem{100, 101, 102}, cursorGetMaxNNextItems(cur, 4))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(1, cur.idx)
	assert.Equal([]sequenceItem{101}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(1, cur.idx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(1, cur.idx)
	assert.Equal([]sequenceItem{101, 102}, cursorGetMaxNNextItems(cur, 3))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(2, cur.idx)
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(2, cur.idx)
	assert.Equal([]sequenceItem{102}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(2, cur.idx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 0))
	assert.Equal(3, cur.idx)
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 1))
	assert.Equal(3, cur.idx)
	assert.Equal([]sequenceItem{}, cursorGetMaxNNextItems(cur, 2))
	assert.Equal(3, cur.idx)
}
