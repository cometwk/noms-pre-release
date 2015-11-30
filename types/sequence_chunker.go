package types

import (
	"github.com/attic-labs/noms/d"
)

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
	cur                        sequenceCursor
	parent                     *sequenceChunker
	last                       sequenceItem
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
type normalizeChunkFn func(prev sequenceItem, values []sequenceItem) []sequenceItem

func normalizeChunkNoop(prev sequenceItem, values []sequenceItem) []sequenceItem {
	return values
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
		if cur.getParent() != nil {
			seq.createParent()
			// We're starting at the beginning of the chunk (seq.current=... below), so skip on the parent immediately.
			//seq.parent.Skip() // TODO actually this isn't true anymore because of the way that Skip() is being called for every chunk fragment in doneWithChunk()
		}
		// Prime the chunker into the state it would be if all items in the sequence had been appended one at a time.
		// TODO should this be WindowSize()-1?
		// TODO I think this needs to be nzeChunk(nil, ...).
		// TODO Regarding nzeChunk, this might suffer from the problem below: that we may be normalizing across several chunks, and the offsets are reset on every chunk, but normalization will continue to be subtracted. How does this work?
		for _, item := range nzeChunk(nil, cursorGetMaxNPrevItems(cur, boundaryChk.WindowSize()-1)) {
			boundaryChk.Write(item)
		}
		// Reconstruct this entire chunk.
		seq.current = nzeChunk(nil, cursorGetMaxNPrevItems(cur, cur.indexInChunk()))
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
	var curParent sequenceCursor
	if seq.cur != nil && seq.cur.getParent() != nil { // getParent() will be nil if seq.cur points to the top of the chunked tree
		curParent = seq.cur.getParent().clone()
	}
	seq.parent = newSequenceChunker(curParent, seq.parentMakeChunk, seq.parentMakeChunk, seq.parentNzeChunk, seq.parentNzeChunk, seq.newBoundaryChecker(), seq.newBoundaryChecker)
}

func (seq *sequenceChunker) commitPendingFirst() {
	d.Chk.True(seq.pendingFirst != nil)
	chunk, _ := seq.makeChunk(seq.pendingFirst)
	seq.last = chunk
	seq.parent.Append(chunk)
	seq.pendingFirst = nil
}

func (seq *sequenceChunker) handleChunkBoundary() {
	d.Chk.True(len(seq.current) > 0)
	if seq.parent == nil {
		seq.pendingFirst = seq.current
	} else {
		chunk, _ := seq.makeChunk(seq.current)
		seq.last = chunk
		seq.parent.Append(chunk)
	}
	seq.current = []sequenceItem{}
}

func (seq *sequenceChunker) doneWithChunk() (sequenceItem, Value) {
	if seq.cur != nil {
		remainder := cursorGetMaxNNextItems(seq.cur, seq.boundaryChk.WindowSize()-1) // TODO WindowSize()-1 ?
		// Carve up the remainder into chunks so that each can be normalized.
		boundaryDetector := seq.cur.clone()
		var chunks [][]sequenceItem
		var chunk []sequenceItem
		prev, _ := seq.cur.prevInChunk() // |prev| can be nil, but that's ok
		for _, n := range remainder {
			if chunk != nil && boundaryDetector.indexInChunk() == 0 {
				chunks = append(chunks, seq.nzeChunk(prev, chunk))
				chunk = nil
				prev = nil
			}
			chunk = append(chunk, n)
			boundaryDetector.advance()
		}
		chunks = append(chunks, seq.nzeChunk(prev, chunk))
		// and now we chunk.
		for _, chunk := range chunks {
			if seq.parent != nil {
				seq.parent.Skip()
			}
			for _, n := range chunk {
				seq.Append(n)
			}
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
