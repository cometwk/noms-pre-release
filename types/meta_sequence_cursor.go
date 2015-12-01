package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
)

// TODO document return type
func newMetaSequenceCursor(root metaSequence, cs chunks.ChunkSource) (sequenceCursor, Value) {
	d.Chk.NotNil(root)

	getItem := func(item sequenceCursorItem, idx int) sequenceItem {
		return item.(metaSequence).tupleAt(idx)
	}
	readChunk := func(item sequenceItem) (sequenceCursorItem, int) {
		ms := ReadValue(item.(metaTuple).ref, cs).(metaSequence)
		return ms, ms.tupleCount()
	}

	cursors := []*sequenceChunkerCursor{&sequenceChunkerCursor{nil, root, 0, root.tupleCount(), getItem, readChunk}}
	for {
		cursor := cursors[len(cursors)-1]
		mt, ok := cursor.current()
		d.Chk.True(ok)
		val := ReadValue(mt.(metaTuple).ref, cs)
		if ms, ok := val.(metaSequence); ok {
			cursors = append(cursors, &sequenceChunkerCursor{cursor, ms, 0, ms.tupleCount(), getItem, readChunk})
		} else {
			return cursor, val
		}
	}

	panic("not reachable")
}

// TODO move these somewhere else?
type cursorIterFn func(v Value) bool

func iterateMetaSequenceLeaf(root metaSequence, cs chunks.ChunkSource, cb cursorIterFn) {
	cursor, v := newMetaSequenceCursor(root, cs)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		mt, ok := cursor.current()
		d.Chk.True(ok)
		v = ReadValue(mt.(metaTuple).ref, cs)
	}

	panic("not reachable")
}
