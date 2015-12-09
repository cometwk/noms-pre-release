package types

import "github.com/attic-labs/noms/d"

type sequenceItem interface{}

type boundaryChecker interface {
	// Write takes an item and returns true if the sequence should chunk after this item, false if not.
	Write(sequenceItem) bool
	// WindowSize returns the minimum number of items in a stream that must be written before resuming a chunking sequence.
	WindowSize() int
}

type newBoundaryCheckerFn func() boundaryChecker

type sequenceChunker struct {
	cur                        *sequenceCursor
	parent                     *sequenceChunker
	current, pendingFirst      []sequenceItem
	makeChunk, parentMakeChunk makeChunkFn
	boundaryChk                boundaryChecker
	newBoundaryChecker         newBoundaryCheckerFn
	used                       bool
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (sequenceItem, Value)

func newEmptySequenceChunker(makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	return newSequenceChunker(nil, makeChunk, parentMakeChunk, boundaryChk, newBoundaryChecker)
}

func newSequenceChunker(cur *sequenceCursor, makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.
	d.Chk.NotNil(makeChunk)
	d.Chk.NotNil(parentMakeChunk)
	d.Chk.NotNil(boundaryChk)
	d.Chk.NotNil(newBoundaryChecker)

	seq := &sequenceChunker{
		cur,
		nil,
		[]sequenceItem{}, nil,
		makeChunk, parentMakeChunk,
		boundaryChk,
		newBoundaryChecker,
		false,
	}

	if cur != nil {
		// Eagerly create a chunker for each level of the existing tree, but note that we may not necessarily need them all, since chunk boundaries may change such that the tree ends up shallower. The |seq.used| flag accounts for that case.
		if cur.parent != nil {
			seq.createParent()
		}
		// Prime the chunker into the state it would be if all items in the sequence had been appended one at a time.
		for _, item := range cur.maxNPrevItems(boundaryChk.WindowSize() - 1) {
			boundaryChk.Write(item)
		}
		// Reconstruct this entire chunk.
		seq.current = cur.maxNPrevItems(cur.indexInChunk())
		seq.used = len(seq.current) > 0
	}

	return seq
}

func (seq *sequenceChunker) Append(item sequenceItem) {
	d.Chk.NotNil(item)
	// Checking for seq.pendingFirst must happen immediately, because it's effectively a continuation from the last call to Append. Specifically, if the last call to Append created the first chunk boundary, delay creating the parent until absolutely necessary. Otherwise, we will be in a state where a parent has only a single item, which is invalid.
	if seq.pendingFirst != nil {
		seq.createParent()
		seq.commitPendingFirst()
	}
	seq.current = append(seq.current, item)
	seq.used = true
	if seq.boundaryChk.Write(item) {
		seq.handleChunkBoundary()
	}
}

func (seq *sequenceChunker) Skip() {
	if seq.cur != nil && seq.cur.advance() && seq.cur.indexInChunk() == 0 {
		// Advancing moved our cursor into the next chunk. We need to advance our parent's cursor, so that when our parent writes out the remaining chunks it doesn't include the chunk that we skipped.
		seq.skipParentIfExists()
	}
}

func (seq *sequenceChunker) skipParentIfExists() {
	if seq.parent != nil {
		seq.parent.Skip()
	}
}

func (seq *sequenceChunker) createParent() {
	d.Chk.True(seq.parent == nil)
	var parent *sequenceCursor
	if seq.cur != nil && seq.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip implements this itself.
		parent = seq.cur.parent.clone()
	}
	seq.parent = newSequenceChunker(parent, seq.parentMakeChunk, seq.parentMakeChunk, seq.newBoundaryChecker(), seq.newBoundaryChecker)
}

func (seq *sequenceChunker) commitPendingFirst() {
	d.Chk.True(seq.pendingFirst != nil)
	chunk, _ := seq.makeChunk(seq.pendingFirst)
	seq.parent.Append(chunk)
	seq.pendingFirst = nil
}

func (seq *sequenceChunker) handleChunkBoundary() {
	d.Chk.True(len(seq.current) > 0)
	if seq.parent == nil {
		seq.pendingFirst = seq.current
	} else {
		chunk, _ := seq.makeChunk(seq.current)
		seq.parent.Append(chunk)
	}
	seq.current = []sequenceItem{}
}

func (seq *sequenceChunker) Done() Value {
	if seq.cur != nil {
		// Append the rest of the values in the sequence, up to the window size (plus its chunk). It needs to be the full window size because anything that was appended in the meantime will have changed the boundary chunker.
		nextChunks := seq.cur.chunksWithNNextItems(seq.boundaryChk.WindowSize())
		if len(nextChunks) == 0 {
			// If there are no next chunks it means the cursor was past the end, but due to the way cursors work, the parent cursor will actually point to its last chunk. We need to force it to point past the end so that our parent's Done() method doesn't add the last chunk twice.
			seq.skipParentIfExists()
		}
		for _, chunk := range nextChunks {
			// Every chunk we repopulate from our cursor is a chunk that the parent should not take from its cursor.
			seq.skipParentIfExists()
			for _, v := range chunk {
				seq.Append(v)
			}
		}
	}

	if seq.pendingFirst != nil {
		d.Chk.True(seq.parent == nil)
		d.Chk.Equal(0, len(seq.current))
		_, done := seq.makeChunk(seq.pendingFirst)
		return done
	}

	if seq.parent != nil && seq.parent.used {
		if len(seq.current) > 0 {
			seq.handleChunkBoundary()
		}
		return seq.parent.Done()
	}

	_, done := seq.makeChunk(seq.current)
	return done
}
