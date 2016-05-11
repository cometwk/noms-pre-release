package types

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

const chunksDiffTestSize = 50000

func assertDiffIsConsistent(t *testing.T, vr ValueReader, rootA, rootB Ref) {
	assert := assert.New(t)

	var getChunkGraph func(r Ref) []Ref
	getChunkGraph = func(r Ref) []Ref {
		res := []Ref{r}
		if chunks := r.TargetValue(vr).Chunks(); chunks != nil {
			for _, chunk := range chunks {
				res = append(res, getChunkGraph(chunk)...)
			}
		}
		return res
	}

	type stringSet map[string]struct{}

	refsToStringSet := func(refs []Ref) stringSet {
		s := stringSet{}
		for _, r := range refs {
			s[r.TargetRef().String()] = struct{}{}
		}
		return s
	}

	graphA := refsToStringSet(getChunkGraph(rootA))
	graphB := refsToStringSet(getChunkGraph(rootB))

	diffA, diffB := ChunksDiff(vr, vr, rootA, rootB)
	onlyA, onlyB := refsToStringSet(diffA), refsToStringSet(diffB)

	for r, _ := range onlyA {
		_, ok := graphA[r]
		assert.True(ok, "graphA is missing %s", r)
		_, ok = graphB[r]
		assert.False(ok, "graphB should not contain %s", r)
	}

	for r, _ := range onlyB {
		_, ok := graphB[r]
		assert.True(ok, "graphB is missing %s", r)
		_, ok = graphA[r]
		assert.False(ok, "graphA should not contain %s", r)
	}

	union := func(s1, s2 stringSet) stringSet {
		u := stringSet{}
		for k, v := range s1 {
			u[k] = v
		}
		for k, v := range s2 {
			u[k] = v
		}
		return u
	}

	assert.Equal(union(graphA, onlyB), union(graphB, onlyA))
}

func numsFromTo(from, to uint64) (res []Value) {
	for i := from; i < to; i++ {
		res = append(res, Number(from))
	}
	return res
}

func testDiffSplice(t *testing.T, idx, del uint64, vals []Value) {
	vr := NewTestValueStore()
	list := NewList(numsFromTo(0, chunksDiffTestSize)...)
	splice := list.Remove(idx, idx+del).Insert(idx, vals...)
	assertDiffIsConsistent(t, vr, vr.WriteValue(list), vr.WriteValue(splice))
}

func TestChunksDiffEmptyDiff(t *testing.T) {
	vr := NewTestValueStore()
	list := NewList(numsFromTo(0, chunksDiffTestSize)...)
	listRef := vr.WriteValue(list)
	assertDiffIsConsistent(t, vr, listRef, vr.WriteValue(NewList()))
}

func TestChunksDiffSame(t *testing.T) {
	testDiffSplice(t, 0, 0, []Value{})
}

func TestChunksDiffSmallNumberOfItems(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	next10 := numsFromTo(chunksDiffTestSize, chunksDiffTestSize+10)

	// Start
	testDiffSplice(t, 0, 0, next10)
	testDiffSplice(t, 0, 10, []Value{})
	testDiffSplice(t, 0, 10, next10)

	// Middle
	testDiffSplice(t, chunksDiffTestSize/2, 0, next10)
	testDiffSplice(t, chunksDiffTestSize/2, 10, []Value{})
	testDiffSplice(t, chunksDiffTestSize/2, 10, next10)

	// End
	testDiffSplice(t, chunksDiffTestSize, 0, next10)
	testDiffSplice(t, chunksDiffTestSize-10, 10, []Value{})
	testDiffSplice(t, chunksDiffTestSize-10, 10, next10)
}

func TestChunksDiffHalfNumberOfItems(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	half := numsFromTo(chunksDiffTestSize, chunksDiffTestSize+chunksDiffTestSize/2)

	// Start
	testDiffSplice(t, 0, 0, half)
	testDiffSplice(t, 0, chunksDiffTestSize/2, []Value{})
	testDiffSplice(t, 0, chunksDiffTestSize/2, half)

	// Middle
	testDiffSplice(t, chunksDiffTestSize/4, 0, half)
	testDiffSplice(t, chunksDiffTestSize/4, chunksDiffTestSize/2, []Value{})
	testDiffSplice(t, chunksDiffTestSize/4, chunksDiffTestSize/2, half)

	// End
	testDiffSplice(t, chunksDiffTestSize, 0, half)
	testDiffSplice(t, chunksDiffTestSize/2, chunksDiffTestSize/2, []Value{})
	testDiffSplice(t, chunksDiffTestSize/2, chunksDiffTestSize/2, half)
}

func TestChunksDiffRefHeap(t *testing.T) {
	unique := 0
	newRefWithHeight := func(height uint64) Ref {
		r := NewTypedRefFromValue(Number(unique))
		unique++
		r.height = height
		return r
	}

	assert := assert.New(t)

	h := refHeap{}
	heap.Init(&h)

	r1 := newRefWithHeight(1)
	r2 := newRefWithHeight(2)
	r3 := newRefWithHeight(3)

	heap.Push(&h, r1)
	assert.Equal(r1, h[0])
	assert.Equal(1, len(h))

	heap.Push(&h, r3)
	assert.Equal(r3, h[0])
	assert.Equal(2, len(h))

	heap.Push(&h, r2)
	assert.Equal(r3, h[0])
	assert.Equal(3, len(h))

	assert.Equal(r3, heap.Pop(&h).(Ref))
	assert.Equal(r2, h[0])
	assert.Equal(2, len(h))

	assert.Equal(r2, heap.Pop(&h).(Ref))
	assert.Equal(r1, h[0])
	assert.Equal(1, len(h))

	assert.Equal(r1, heap.Pop(&h).(Ref))
	assert.Equal(0, len(h))
}
