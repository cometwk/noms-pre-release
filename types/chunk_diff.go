package types

// TODO: rename file to chunks_diff/chunks_diff_test.go.

import (
	"container/heap"
	"sort"
)

// refHeap implements heap.Interface (which includes sort.Interface) as a height based priority queue.
type refHeap []Ref

func (h refHeap) Head() Ref {
	return h[0]
}

func (h refHeap) Len() int {
	return len(h)
}

func (h refHeap) Less(i, j int) bool {
	// > because we want the larger heights to be at the start of the queue.
	return h[i].Height() > h[j].Height()
}

func (h refHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *refHeap) Push(r interface{}) {
	*h = append(*h, r.(Ref))
}

func (h *refHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// refSlice implements sort.Interface to order by target ref.
type refSlice []Ref

func (s refSlice) Len() int {
	return len(s)
}

func (s refSlice) Less(i, j int) bool {
	return s[i].TargetRef().Less(s[j].TargetRef())
}

func (s refSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// TODO: option to exclude one of the result tuples (e.g. might only care about chunks in rootA that aren't in rootB).
// TODO: support concurrency. The JS implementation will use Promises; can we use Go to implement something similar?
func ChunksDiff(vrA, vrB ValueReader, rootA, rootB Ref) ([]Ref, []Ref) {

	// TODO: comment.
	commit := func(vr ValueReader, onlyIn refSlice, reachable *refHeap, r Ref) refSlice {
		if chunks := r.TargetValue(vr).Chunks(); chunks != nil {
			for _, chunk := range chunks {
				heap.Push(reachable, chunk)
			}
		}
		return append(onlyIn, r)
	}

	// TODO: comment.
	syncTo := func(vr ValueReader, onlyIn refSlice, reachable *refHeap, height uint64) refSlice {
		for reachable.Len() > 0 && reachable.Head().Height() > height {
			onlyIn = commit(vr, onlyIn, reachable, heap.Pop(reachable).(Ref))
		}
		return onlyIn
	}

	// TODO: comment.
	popTo := func(reachable *refHeap, height uint64) (res refSlice) {
		for reachable.Len() > 0 && reachable.Head().Height() > height {
			res = append(res, heap.Pop(reachable).(Ref))
		}
		return
	}

	// TODO: comment.
	diffRefSlices := func(sliceA, sliceB refSlice) (refSlice, refSlice) {
		onlyInA, onlyInB := refSlice{}, refSlice{}
		idxA, idxB := uint64(0), uint64(0)

		sort.Sort(sliceA)
		sort.Sort(sliceB)

		for idxA < uint64(len(sliceA)) && idxB < uint64(len(sliceB)) {
			refA, refB := sliceA[idxA], sliceB[idxB]
			switch {
			case refA.TargetRef() == refB.TargetRef():
				idxA++
				idxB++
			case refA.TargetRef().Less(refB.TargetRef()):
				onlyInA = append(onlyInA, refA)
				idxA++
			default:
				onlyInB = append(onlyInB, refB)
				idxB++
			}
		}

		onlyInA = append(onlyInA, sliceA[idxA:]...)
		onlyInB = append(onlyInB, sliceB[idxB:]...)
		return onlyInA, onlyInB
	}

	onlyInA, onlyInB := refSlice{}, refSlice{}
	reachableFromA, reachableFromB := refHeap{}, refHeap{}

	heap.Init(&reachableFromA)
	heap.Init(&reachableFromB)
	heap.Push(&reachableFromA, rootA)
	heap.Push(&reachableFromB, rootB)

	for len(reachableFromA) > 0 && len(reachableFromB) > 0 {
		heightA := reachableFromA[0].Height()
		heightB := reachableFromB[0].Height()

		if heightA > heightB {
			onlyInA = syncTo(vrA, onlyInA, &reachableFromA, heightB)
		} else if heightB > heightA {
			onlyInB = syncTo(vrB, onlyInB, &reachableFromB, heightA)
		} else {
			newRefsA, newRefsB := diffRefSlices(popTo(&reachableFromA, heightA-1), popTo(&reachableFromB, heightB-1))
			for _, r := range newRefsA {
				onlyInA = commit(vrA, onlyInA, &reachableFromA, r)
			}
			for _, r := range newRefsB {
				onlyInB = commit(vrB, onlyInB, &reachableFromB, r)
			}
		}
	}

	onlyInA = append(onlyInA, reachableFromA...)
	onlyInB = append(onlyInB, reachableFromB...)
	return []Ref(onlyInA), []Ref(onlyInB)
}
