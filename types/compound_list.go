package types

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	listWindowSize = 64
	listPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundList struct {
	metaSequenceObject
	ref *ref.Ref
	cs  chunks.ChunkStore
}

func buildCompoundList(tuples metaSequenceData, t Type, cs chunks.ChunkSource) Value {
	return compoundList{metaSequenceObject{tuples, t}, &ref.Ref{}, cs.(chunks.ChunkStore)}
}

func getListSequenceData(v Value) metaSequenceData {
	return v.(compoundList).tuples
}

func init() {
	registerMetaValue(ListKind, buildCompoundList, getListSequenceData)
}

func (cl compoundList) Equals(other Value) bool {
	return other != nil && cl.t.Equals(other.Type()) && cl.Ref() == other.Ref()
}

func (cl compoundList) Ref() ref.Ref {
	return EnsureRef(cl.ref, cl)
}

func (cl compoundList) Len() uint64 {
	return cl.tuples[len(cl.tuples)-1].uint64Value()
}

func (cl compoundList) Empty() bool {
	d.Chk.True(cl.Len() > 0) // A compound object should never be empty.
	return false
}

func (cl compoundList) cursorAt(idx uint64) (cursor sequenceCursor, listLeaf List, start uint64) {
	d.Chk.True(idx <= cl.Len())
	cursor, _ = newMetaSequenceCursor(cl, cl.cs)

	chunkStart := cursor.seek(func(v, parent sequenceCursorItem) bool {
		d.Chk.NotNil(v)
		d.Chk.NotNil(parent)

		// TODO the way that parent is a UInt64 and v is a metaTuple is confusing largely because they're both sequenceCursorItems?
		return idx < uint64(parent.(UInt64))+uint64(v.(metaTuple).value.(UInt64))
	}, func(parent, prev, current sequenceCursorItem) sequenceCursorItem {
		// TODO the way that parent is a UInt64 and prev is a metaTuple is confusing largely because they're both sequenceCursorItems?
		pv := uint64(0)
		if prev != nil {
			pv = uint64(prev.(metaTuple).value.(UInt64))
		}

		return UInt64(uint64(parent.(UInt64)) + pv)
	}, UInt64(0))

	mt, ok := cursor.current()
	d.Chk.True(ok)
	listLeaf = ReadValue(mt.(metaTuple).ref, cl.cs).(List)
	start = uint64(chunkStart.(UInt64))
	return
}

func (cl compoundList) Get(idx uint64) Value {
	_, l, start := cl.cursorAt(idx)
	return l.Get(idx - start)
}

func (cl compoundList) Append(vs ...Value) compoundList {
	return cl.Insert(cl.Len(), vs...)
}

func (cl compoundList) Insert(idx uint64, vs ...Value) compoundList {
	seq := cl.sequenceChunkerAtIndex(idx)
	for _, v := range vs {
		seq.Append(v)
	}
	return seq.Done().(compoundList)
}

func (cl compoundList) Remove(idx uint64) compoundList {
	seq := cl.sequenceChunkerAtIndex(idx)
	seq.Skip()
	return seq.Done().(compoundList)
}

func (cl compoundList) sequenceChunkerAtIndex(idx uint64) *sequenceChunker {
	metaCur, leaf, start := cl.cursorAt(idx)
	seqCur := newSequenceChunkerCursor(metaCur, leaf, int(idx-start), len(leaf.values), getSequenceItemFromList, readListLeafChunkFn(cl.cs))
	return newSequenceChunker(seqCur, makeListLeafChunkFn(cl.t, cl.cs), newMetaSequenceChunkFn(cl.t, cl.cs), normalizeChunkNoop, normalizeMetaSequenceChunk, newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
}

func (cl compoundList) Iter(f listIterFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(List)
		for i, v := range list.values {
			if f(v, start+uint64(i)) {
				return true
			}
		}
		start += list.Len()
		return false
	})

}

func (cl compoundList) IterAll(f listIterAllFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(List)
		for i, v := range list.values {
			f(v, start+uint64(i))
		}
		start += list.Len()
		return false
	})
}

func newListLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(listWindowSize, func(h *buzhash.BuzHash, item sequenceItem) bool {
		v := item.(Value)
		digest := v.Ref().Digest()
		b := digest[0]
		return h.HashByte(b)&listPattern == listPattern
	})
}

func makeListLeafChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		concreteType := t.Desc.(CompoundDesc).ElemTypes[0]
		list := List{values, concreteType, &ref.Ref{}}
		ref := WriteValue(list, cs)
		return metaTuple{ref, UInt64(len(values))}, list
	}
}

func getSequenceItemFromList(curitem sequenceCursorItem, idx int) sequenceItem {
	return curitem.(List).values[idx]
}

func readListLeafChunkFn(cs chunks.ChunkStore) readChunkFn {
	return func(item sequenceItem) (sequenceCursorItem, int) {
		mt := item.(metaTuple)
		list := ReadValue(mt.ref, cs).(List)
		return list, len(list.values)
	}
}

func NewCompoundList(t Type, cs chunks.ChunkStore, values ...Value) Value {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(t, cs), newMetaSequenceChunkFn(t, cs), newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return seq.Done().(Value)
}
