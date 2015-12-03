package types

import "github.com/attic-labs/noms/d"

// TODO put this in a "ptree" directory.

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
	nzeChunk, parentNzeChunk   normalizeChunkFn
	boundaryChk                boundaryChecker
	newBoundaryChecker         newBoundaryCheckerFn
	empty                      bool
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (sequenceItem, Value)

// normalizeChunkFn takes a sequence of existing items |values|, and returns a sequence equivalent as though it had never gone through the chunking progress. |prev| is the last item in the sequence before |values| and may be nil if there is no such item.
// TODO get rid of prev; callers can just pass in a values array of an additional length. it's also important that values only come from the same chunk - this must be commented.
type normalizeChunkFn func(prev sequenceItem, values []sequenceItem) []sequenceItem

func normalizeChunkNoop(prev sequenceItem, values []sequenceItem) []sequenceItem {
	return values
}

func newEmptySequenceChunker(makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	return newSequenceChunker(nil, makeChunk, parentMakeChunk, normalizeChunkNoop, normalizeChunkNoop, boundaryChk, newBoundaryChecker)
}

func newSequenceChunker(cur *sequenceCursor, makeChunk, parentMakeChunk makeChunkFn, nzeChunk, parentNzeChunk normalizeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
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
		true,
	}

	if cur != nil {
		// Eagerly create a chunker for each level of the existing tree. This is correct while sequences can only ever append, and therefore the tree can only ever grow in height, but generally speaking the tree can also shrink - due to both removals and changes - and in that situation we can't simply create every meta-node that was in the cursor. If we did that, we'd end up with meta-nodes with only a single entry, which is illegal.
		if cur.parent != nil {
			seq.createParent()
		}
		// Prime the chunker into the state it would be if all items in the sequence had been appended one at a time.
		// XXX I think this is wrong, it might cross chunk boundaries, which would have reset when constructing the list, but won't reset here. An appropriate way to fix this would be for cursorGetMaxNPrevItems to return a [][]sequenceItem then for this code to be prevItems := cursorGetMaxNPrevItems(); for i, chunk := range prevItems; seq.current = append(seq.current, nzeChunk(prevItems[i-1][0], chunk)), more or less.
		for _, item := range nzeChunk(nil, cur.maxNPrevItems(boundaryChk.WindowSize()-1)) { // TODO I don't think this should be nil
			boundaryChk.Write(item)
		}
		// Reconstruct this entire chunk. The previous value is nil because this is the start of the chunk.
		seq.current = nzeChunk(nil, cur.maxNPrevItems(cur.indexInChunk()))
		seq.empty = len(seq.current) == 0
	}

	return seq
}

func (seq *sequenceChunker) Append(item sequenceItem) {
	d.Chk.NotNil(item)
	// Checking for seq.pendingFirst must happen immediately, because it's effectively a continuation from the last call to Append. Specifically, if the last call to Append created the first chunk boundary, delay creating the parent until absolutely necessary. Otherwise, we will be in a state where a parent has only a single item, which is invalid.
	if seq.pendingFirst != nil {
		seq.createParent()
		seq.commitPendingFirst()
		d.Chk.True(seq.pendingFirst == nil)
	}
	seq.current = append(seq.current, item)
	seq.empty = false
	if seq.boundaryChk.Write(item) {
		seq.handleChunkBoundary()
	}
}

func (seq *sequenceChunker) Skip() {
	if seq.cur != nil {
		seq.cur.advance()
	}
}

func (seq *sequenceChunker) createParent() {
	d.Chk.True(seq.parent == nil)
	var parent *sequenceCursor
	if seq.cur != nil && seq.cur.parent != nil { // seq.cur.parent will be nil if seq.cur points to the top of the chunked tree
		parent = seq.cur.parent.clone()
	}
	seq.parent = newSequenceChunker(parent, seq.parentMakeChunk, seq.parentMakeChunk, seq.parentNzeChunk, seq.parentNzeChunk, seq.newBoundaryChecker(), seq.newBoundaryChecker)
	// Skip the immediate parent of this cursor, which will no longer be valid. Chunks should be appended to the parent starting after it.
	seq.parent.Skip()
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

func (seq *sequenceChunker) doneWithChunk() (sequenceItem, Value) {
	if seq.cur != nil {
		// TODO go back to a prevInChunk() method? use that everywhere with nzeChunk, clearer than assuming nil, though I could assert that.
		var prev sequenceItem
		if seq.cur.indexInChunk() > 0 {
			curToPrev := seq.cur.clone()
			d.Chk.True(curToPrev.retreat())
			prev = curToPrev.current()
		}
		for i, chunk := range seq.cur.maxNNextItems(seq.boundaryChk.WindowSize() - 1) {
			// Don't Skip while pushing up the first chunk, it was already skipped in createParent().
			if i > 0 && seq.parent != nil {
				seq.parent.Skip()
			}
			for _, v := range seq.nzeChunk(prev, chunk) {
				seq.Append(v)
			}
			prev = nil
		}
	}
	if seq.pendingFirst != nil {
		d.Chk.True(seq.parent == nil)
		d.Chk.Equal(0, len(seq.current))
		return seq.makeChunk(seq.pendingFirst)
	}
	// TODO this "seq.parent.empty" is an unfortunate hack. it would be better to just only construct the parent at Append time.
	if seq.parent != nil && !seq.parent.empty {
		if len(seq.current) > 0 {
			seq.handleChunkBoundary()
		}
		return seq.parent.doneWithChunk()
	}
	return seq.makeChunk(seq.current)
}

func (seq *sequenceChunker) Done() Value {
	_, done := seq.doneWithChunk()
	return done
}
