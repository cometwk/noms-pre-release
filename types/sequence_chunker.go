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
	cur                        sequenceCursor
	parent                     *sequenceChunker
	current, pendingFirst      []sequenceItem
	makeChunk, parentMakeChunk makeChunkFn
	nzeChunk, parentNzeChunk   normalizeChunkFn
	boundaryChk                boundaryChecker
	newBoundaryChecker         newBoundaryCheckerFn
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (sequenceItem, Value)

// normalizeChunkFn takes a sequence of existing items, and returns a sequence equivalent as though it had never gone through the chunking progress.
type normalizeChunkFn func(values []sequenceItem) []sequenceItem

func normalizeChunkNoop(si []sequenceItem) []sequenceItem {
	return si
}

func newEmptySequenceChunker(makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	return newSequenceChunker(nil, makeChunk, parentMakeChunk, normalizeChunkNoop, normalizeChunkNoop, boundaryChk, newBoundaryChecker)
}

func newSequenceChunker(cur sequenceCursor, makeChunk, parentMakeChunk makeChunkFn, nzeChunk, parentNzeChunk normalizeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	d.Chk.NotNil(makeChunk)
	d.Chk.NotNil(parentMakeChunk)
	d.Chk.NotNil(nzeChunk)
	d.Chk.NotNil(parentNzeChunk)
	d.Chk.NotNil(boundaryChk)
	d.Chk.NotNil(newBoundaryChecker)

	seq := &sequenceChunker{
		cur,
		nil,
		[]sequenceItem{}, nil,
		makeChunk, parentMakeChunk,
		nzeChunk, parentNzeChunk,
		boundaryChk,
		newBoundaryChecker,
	}

	if cur != nil {
		// Eagerly create a chunker for each level of the existing tree. This is correct while sequences can only ever append, and therefore the tree can only ever grow in height, but generally speaking the tree can also shrink - due to both removals and changes - and in that situation we can't simply create every meta-node that was in the cursor. If we did that, we'd end up with meta-nodes with only a single entry, which is illegal.
		if cur.getParent() != nil {
			seq.createParent()
		}
		// Prime the chunker into the state it would be if all items in the sequence had been appended one at a time.
		for _, item := range cursorGetMaxNPrevItems(cur, boundaryChk.WindowSize()) {
			boundaryChk.Write(item)
		}
		// Reconstruct this entire chunk.
		seq.current = nzeChunk(cursorGetMaxNPrevItems(cur, cur.indexInChunk()))
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
	if seq.boundaryChk.Write(item) {
		seq.handleChunkBoundary()
	}
}

func (seq *sequenceChunker) Skip() {
}

func (seq *sequenceChunker) createParent() {
	d.Chk.True(seq.parent == nil)
	var curParent sequenceCursor
	if seq.cur != nil {
		d.Chk.NotNil(seq.cur)
		curParent = seq.cur.getParent() // this will be nil if seq.cur points to the top of the chunked tree
	}
	seq.parent = newSequenceChunker(curParent, seq.parentMakeChunk, seq.parentMakeChunk, seq.parentNzeChunk, seq.parentNzeChunk, seq.newBoundaryChecker(), seq.newBoundaryChecker)
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
		// TODO:
		// The algorithm only needs to advance past the window size, then fill in the rest of the chunk it ends up in.
		// It needs to call Skip() on the parent each time it passes over a chunk boundary.
		/*
			items, boundaries := cursorGetMaxNNextItems(seq.cur, seq.boundaryChk.WindowSize())
			for i, item := range items {
				if _, ok := boundaries[i]; ok {
					seq.Skip()
				}
				seq.Append(item)
			}
		*/
	}
	if seq.pendingFirst != nil {
		d.Chk.True(seq.parent == nil)
		d.Chk.Equal(0, len(seq.current))
		_, done := seq.makeChunk(seq.pendingFirst)
		return done
	}
	if seq.parent != nil {
		if len(seq.current) > 0 {
			seq.handleChunkBoundary()
		}
		return seq.parent.Done()
	}
	_, done := seq.makeChunk(seq.current)
	return done
}
