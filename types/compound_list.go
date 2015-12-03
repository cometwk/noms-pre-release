package types

import (
	"crypto/sha1"

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

func (cl compoundList) cursorAt(idx uint64) (cursor *sequenceCursor, listLeaf List, start uint64) {
	d.Chk.True(idx <= cl.Len())
	cursor, _ = newMetaSequenceCursor(cl, cl.cs)

	chunkStart := cursor.seek(func(carry interface{}, mt sequenceItem) bool {
		d.Chk.NotNil(mt)
		d.Chk.NotNil(carry)

		return idx < uint64(carry.(UInt64))+uint64(mt.(metaTuple).value.(UInt64))
	}, func(carry interface{}, prev, current sequenceItem) interface{} {
		pv := uint64(0)
		if prev != nil {
			pv = uint64(prev.(metaTuple).value.(UInt64))
		}

		return UInt64(uint64(carry.(UInt64)) + pv)
	}, UInt64(0))

	listLeaf = ReadValue(cursor.current().(metaTuple).ref, cl.cs).(List)
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

func (cl compoundList) Remove(start, end uint64) compoundList {
	seq := cl.sequenceChunkerAtIndex(start)
	for i := start; i < end; i++ {
		seq.Skip()
	}
	return seq.Done().(compoundList)
}

func (cl compoundList) RemoveAt(idx uint64) compoundList {
	return cl.Remove(idx, idx+1)
}

func (cl compoundList) sequenceChunkerAtIndex(idx uint64) *sequenceChunker {
	metaCur, leaf, start := cl.cursorAt(idx)
	cur := newSequenceCursor(metaCur, leaf, int(idx-start), len(leaf.values), func(list sequenceItem, idx int) sequenceItem {
		return list.(List).values[idx]
	}, func(mt sequenceItem) (sequenceItem, int) {
		list := ReadValue(mt.(metaTuple).ref, cl.cs).(List)
		return list, len(list.values)
	})
	return newSequenceChunker(cur, makeListLeafChunkFn(cl.t, cl.cs), newMetaSequenceChunkFn(cl.t, cl.cs), normalizeChunkNoop, normalizeMetaSequenceChunk, newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
}

func (cl compoundList) Iter(f listIterFunc) {
	start := uint64(0)

	cl.iterateMetaSequenceLeaf(func(l Value) bool {
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

	cl.iterateMetaSequenceLeaf(func(l Value) bool {
		list := l.(List)
		for i, v := range list.values {
			f(v, start+uint64(i))
		}
		start += list.Len()
		return false
	})
}

// TODO this really would be simpler if IterAll just called Iter and returned false.
func (cl compoundList) iterateMetaSequenceLeaf(cb func(Value) bool) {
	cursor, v := newMetaSequenceCursor(cl, cl.cs)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		v = ReadValue(cursor.current().(metaTuple).ref, cl.cs)
	}

	panic("not reachable")
}

func newListLeafBoundaryChecker() boundaryChecker {
	// TODO: solve the mystery of why the boundary checking isn't idempotent.
	return newBuzHashBoundaryChecker(listWindowSize, sha1.Size, func(h *buzhash.BuzHash, item sequenceItem) bool {
		digest := item.(Value).Ref().Digest()
		_, err := h.Write(digest[:])
		d.Chk.NoError(err)
		return h.Sum32()&listPattern == listPattern
		//return h.HashByte(digest[0])&listPattern == listPattern
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

func NewCompoundList(t Type, cs chunks.ChunkStore, values ...Value) Value {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(t, cs), newMetaSequenceChunkFn(t, cs), newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return seq.Done().(Value)
}
