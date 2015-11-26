package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/d"
)

// TODO test various window sizes: 1, 2, 3, ..., len.
// TODO test normalization
// TODO test with a hash function that takes the window into account, in order to better test the window

type modBoundaryChecker struct {
	mod int
}

func (b modBoundaryChecker) Write(item sequenceItem) bool {
	switch item := item.(type) {
	case int:
		return item%b.mod == 0
	case testSequenceNode:
		return item.sum%b.mod == 0
	}
	panic("not reachable")
}

func (b modBoundaryChecker) WindowSize() int {
	return 3
}

type configurableBoundaryChecker struct {
	boundaries map[int]bool
}

func (b configurableBoundaryChecker) Write(item sequenceItem) (is bool) {
	switch item := item.(type) {
	case int:
		_, is = b.boundaries[item]
	case testSequenceNode:
		_, is = b.boundaries[item.sum]
	default:
		panic("not reachable")
	}
	return
}

func (b configurableBoundaryChecker) WindowSize() int {
	return 3
}

type testSequenceNode struct {
	sum      int
	children []testSequenceNode
}

func listFromInts(ints []int) List {
	vals := make([]Value, len(ints))
	for i, v := range ints {
		vals[i] = Int64(v)
	}

	return NewList(vals...)
}

func makeChunkFromSum(items []sequenceItem) (sequenceItem, Value) {
	d.Chk.True(len(items) > 0)
	sum := 0
	ints := make([]int, len(items))
	nodes := make([]testSequenceNode, len(items))
	for i, item := range items {
		switch item := item.(type) {
		case int:
			sum += item
			ints[i] = item
			nodes[i] = testSequenceNode{item, nil}
		case testSequenceNode:
			sum += item.sum
			ints[i] = item.sum
			nodes[i] = item
		}
	}
	// TODO I'm adding +1 now. Renames needed.
	return testSequenceNode{sum + 1, nodes}, listFromInts(ints)
}

func buildSequenceCursorForTest(node testSequenceNode) (res sequenceCursor) {
	childrenAsSequenceItems := func(n testSequenceNode) (items []sequenceItem) {
		for _, child := range n.children {
			items = append(items, child)
		}
		return
	}

	for ; node.children != nil; node = node.children[0] {
		next := newSequenceChunkerCursor(res, childrenAsSequenceItems(node), 0, func(item sequenceItem) []sequenceItem {
			return childrenAsSequenceItems(item.(testSequenceNode))
		})
		res = next
	}
	d.Chk.True(res != nil)
	return res
}

func fromTo(from, to int) (out []int) {
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return
}

func TestSequenceChunkerAppend(t *testing.T) {
	assert := assert.New(t)

	boundaries := map[int]bool{
		25: true,
		49: true,
	}

	testChunking := func(expect []int, items ...int) {
		seq := newEmptySequenceChunker(makeChunkFromSum, makeChunkFromSum, modBoundaryChecker{3}, func() boundaryChecker { return configurableBoundaryChecker{boundaries} })
		for _, item := range items {
			seq.Append(item)
		}
		dat, done := seq.doneWithChunk()

		assert.True(listFromInts(expect).Equals(done), fmt.Sprintf("%+v != %+v", expect, dat))
	}

	// [1] is not a chunk boundary, so it won't chunk.
	testChunking([]int{1}, fromTo(1, 1)...)

	// [3] is a chunk boundary, but only a single chunk, so treat it as though it didn't chunk.
	testChunking([]int{3}, fromTo(3, 3)...)

	// None of [1, 2] is a chunk boundary, so it won't chunk.
	testChunking([]int{1, 2}, fromTo(1, 2)...)

	// [3, 4] has a chunk boundary on 3, so it should chunk as [3] [4].
	testChunking([]int{4, 5}, fromTo(3, 4)...)

	// [1, 2, 3] ends in a chunk boundary 3, but only a single chunk, so treat is as though it didn't chunk.
	testChunking([]int{1, 2, 3}, fromTo(1, 3)...)

	// [1, 2, 3, 4] has a chunk boundary on 3, so should chunk as [1, 2, 3] [4].
	testChunking([]int{7, 5}, fromTo(1, 4)...)

	// [1, 2, 3, 4, 5, 6, 7, 8, 9] has a chunk boundary on 3/6/9, so should chunk as [1, 2, 3] [4, 5, 6] [7, 8, 9] which sum+1s as [7, 16, 25] which chunks on the 25. Since the chunk is the last entry, it doesn't actually create a parent chunk.
	testChunking([]int{7, 16, 25}, fromTo(1, 9)...)

	// [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] has a chunk boundary on 3/6/9, so should chunk as [1, 2, 3] [4, 5, 6] [7, 8, 9] [10] which sum+1s as [7, 16, 25, 11] which chunks on the 25. This produces chunks of [7, 16, 25] [11] which sum+1s as [49, 12], which chunks as [49] [12] leaving a single sum+1 chunk [62].
	testChunking([]int{50, 13}, fromTo(1, 10)...)
}

func TestSequenceChunkerPrepend(t *testing.T) {
	assert := assert.New(t)

	boundaries := map[int]bool{
		25: true,
		49: true,
	}

	testChunking := func(expect []int, from, to int) {
		newChunker := func(cur sequenceCursor) *sequenceChunker {
			return newSequenceChunker(cur, makeChunkFromSum, makeChunkFromSum, normalizeChunkNoop, normalizeChunkNoop, normalizeChunkNoop, normalizeChunkNoop, modBoundaryChecker{3}, func() boundaryChecker { return configurableBoundaryChecker{boundaries} })
		}
		seq := newChunker(nil)

		// Build a sequence of all but the first item, then prepend the first item.
		// TODO could include this as part of TestSequenceChunkerAppend, in fact, could do all variations of this (insert at pos 0, pos 1, post len-1, etc).
		for i := from + 1; i <= to; i++ {
			seq.Append(i)
		}
		seqItem, _ := seq.doneWithChunk()
		seq = newChunker(buildSequenceCursorForTest(seqItem.(testSequenceNode)))
		seq.Append(from)

		seqItem, done := seq.doneWithChunk()
		assert.True(listFromInts(expect).Equals(done), fmt.Sprintf("%+v != %+v", expect, seqItem))
		// TODO test len is consistent
		// TODO test values are consistent
	}

	/*
		// None of [1, 2] is a chunk boundary, so it won't chunk.
		testChunking([]int{1, 2}, 1, 2)

		// [3, 4] has a chunk boundary on 3, so it should chunk as [3] [4].
		testChunking([]int{4, 5}, 3, 4)

		// [1, 2, 3] ends in a chunk boundary 3, but only a single chunk, so treat is as though it didn't chunk.
		testChunking([]int{1, 2, 3}, 1, 3)

		// [1, 2, 3, 4] has a chunk boundary on 3, so should chunk as [1, 2, 3] [4].
		testChunking([]int{7, 5}, 1, 4)

		// [1, 2, 3, 4, 5] -> [1, 2, 3] [4, 5] -> [7, 10]
		testChunking([]int{7, 10}, 1, 5)

		// [1, 2, 3, 4, 5, 6] -> [1, 2, 3] [4, 5, 6] -> [7, 16]
		testChunking([]int{7, 16}, 1, 6)

		// [1, 2, 3, 4, 5, 6, 7] -> [1, 2, 3] [4, 5, 6] [7] -> [7, 16, 8]
	*/
	testChunking([]int{7, 16, 8}, 1, 7)

	/*

		// [1, 2, 3, 4, 5, 6, 7] -> [1, 2, 3] [4, 5, 6] [7, 8] -> [7, 16, 16]
		testChunking([]int{7, 16, 16}, 1, 8)

		// [1, 2, 3, 4, 5, 6, 7, 8, 9] has a chunk boundary on 3/6/9, so should chunk as [1, 2, 3] [4, 5, 6] [7, 8, 9] which sum+1s as [7, 16, 25] which chunks on the 25. Since the chunk is the last entry, it doesn't actually create a parent chunk.
		testChunking([]int{7, 16, 25}, 1, 9)

		// [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] has a chunk boundary on 3/6/9, so should chunk as [1, 2, 3] [4, 5, 6] [7, 8, 9] [10] which sum+1s as [7, 16, 25, 11] which chunks on the 25. This produces chunks of [7, 16, 25] [11] which sum+1s as [49, 12].
		testChunking([]int{50, 13}, 1, 10)
	*/
}
