package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSimpleList []Value

func (tsl testSimpleList) Set(idx int, v Value) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, v)
	res = append(res, tsl[idx+1:]...)
	return
}

func (tsl testSimpleList) Insert(idx int, vs ...Value) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, vs...)
	res = append(res, tsl[idx:]...)
	return
}

func (tsl testSimpleList) Remove(start, end int) (res testSimpleList) {
	res = append(res, tsl[:start]...)
	res = append(res, tsl[end:]...)
	return
}

func (tsl testSimpleList) RemoveAt(idx int) testSimpleList {
	return tsl.Remove(idx, idx+1)
}

func (tsl testSimpleList) ToNomsList(cs chunks.ChunkStore) List {
	return NewList(cs, tsl...)
}

func getTestSimpleListLen() uint64 {
	return uint64(listPattern) * 50
}

func getTestSimpleList() testSimpleList {
	length := int(getTestSimpleListLen())
	s := rand.NewSource(42)
	values := make([]Value, length)
	for i := 0; i < length; i++ {
		values[i] = Int64(s.Int63() & 0xff)
	}

	return values
}

func testSimpleListFromNomsList(list List) (simple testSimpleList) {
	list.IterAll(func(v Value, offset uint64) {
		simple = append(simple, v)
	})
	return
}

func TestCompoundListGet(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	// Incrementing by len(simpleList)/10 because Get() is too slow to run on every index.
	for i := 0; i < len(simpleList); i += len(simpleList) / 10 {
		assert.Equal(simpleList[i], cl.Get(uint64(i)))
	}
}

func TestCompoundListIter(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	expectIdx := uint64(0)
	endAt := getTestSimpleListLen() / 2
	cl.Iter(func(v Value, idx uint64) bool {
		assert.Equal(expectIdx, idx)
		expectIdx++
		assert.Equal(simpleList[idx], v)
		return expectIdx == endAt
	})

	assert.Equal(endAt, expectIdx)
}

func TestCompoundListIterAll(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
	cl := NewTypedList(cs, tr, simpleList...)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList[idx], v)
	})

	assert.Equal(getTestSimpleListLen(), expectIdx)
}

func TestCompoundListLen(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))

	cl := NewTypedList(cs, tr, getTestSimpleList()...).(compoundList)
	assert.Equal(getTestSimpleListLen(), cl.Len())
	cl = NewTypedList(cs, tr, append(getTestSimpleList(), getTestSimpleList()...)...).(compoundList)
	assert.Equal(getTestSimpleListLen()*2, cl.Len())
}

func TestCompoundListCursorAt(t *testing.T) {
	assert := assert.New(t)

	listLen := func(at uint64, next func(*sequenceCursor) bool) (size uint64) {
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		cl := NewTypedList(cs, tr, getTestSimpleList()...).(compoundList)
		cur, _, _ := cl.cursorAt(at)
		for {
			size += readMetaTupleValue(cur.current(), cs).(List).Len()
			if !next(cur) {
				return
			}
		}
		panic("not reachable")
	}

	assert.Equal(getTestSimpleListLen(), listLen(0, func(cur *sequenceCursor) bool {
		return cur.advance()
	}))
	assert.Equal(getTestSimpleListLen(), listLen(getTestSimpleListLen(), func(cur *sequenceCursor) bool {
		return cur.retreat()
	}))
}

func TestCompoundListAppend(t *testing.T) {
	assert := assert.New(t)

	newCompoundList := func(items testSimpleList) compoundList {
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind))
		return NewTypedList(cs, tr, items...).(compoundList)
	}

	compoundToSimple := func(cl List) (simple testSimpleList) {
		cl.IterAll(func(v Value, offset uint64) {
			simple = append(simple, v)
		})
		return
	}

	cl := newCompoundList(getTestSimpleList())
	cl2 := cl.Append(Int64(42))
	cl3 := cl2.Append(Int64(43))
	cl4 := cl3.Append(getTestSimpleList()...)
	cl5 := cl4.Append(Int64(44), Int64(45))
	cl6 := cl5.Append(getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, compoundToSimple(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(newCompoundList(expected).Equals(cl))

	expected = append(expected, Int64(42))
	assert.Equal(expected, compoundToSimple(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(newCompoundList(expected).Equals(cl2))

	expected = append(expected, Int64(43))
	assert.Equal(expected, compoundToSimple(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(newCompoundList(expected).Equals(cl3))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(newCompoundList(expected).Equals(cl4))

	expected = append(expected, Int64(44), Int64(45))
	assert.Equal(expected, compoundToSimple(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(newCompoundList(expected).Equals(cl5))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, compoundToSimple(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(newCompoundList(expected).Equals(cl6))
}

func TestCompoundListInsertNothing(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	cl := getTestSimpleList().ToNomsList(cs)

	assert.True(cl.Equals(cl.Insert(0)))
	for i := uint64(1); i < getTestSimpleListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Insert(i)))
	}
	assert.True(cl.Equals(cl.Insert(cl.Len() - 1)))
	assert.True(cl.Equals(cl.Insert(cl.Len())))
}

func TestCompoundListInsertStart(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	cl := getTestSimpleList().ToNomsList(cs)
	cl2 := cl.Insert(0, Int64(42))
	cl3 := cl2.Insert(0, Int64(43))
	cl4 := cl3.Insert(0, getTestSimpleList()...)
	cl5 := cl4.Insert(0, Int64(44), Int64(45))
	cl6 := cl5.Insert(0, getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl))

	expected = expected.Insert(0, Int64(42))
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl2))

	expected = expected.Insert(0, Int64(43))
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl3))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl4))

	expected = expected.Insert(0, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleListFromNomsList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl5))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl6))
}

func TestCompoundListInsertMiddle(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	cl := getTestSimpleList().ToNomsList(cs)
	cl2 := cl.Insert(100, Int64(42))
	cl3 := cl2.Insert(200, Int64(43))
	cl4 := cl3.Insert(300, getTestSimpleList()...)
	cl5 := cl4.Insert(400, Int64(44), Int64(45))
	cl6 := cl5.Insert(500, getTestSimpleList()...)
	cl7 := cl6.Insert(600, Int64(100))

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl))

	expected = expected.Insert(100, Int64(42))
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()+1, cl2.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl2))

	expected = expected.Insert(200, Int64(43))
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()+2, cl3.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl3))

	expected = expected.Insert(300, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, cl4.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl4))

	expected = expected.Insert(400, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleListFromNomsList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, cl5.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl5))

	expected = expected.Insert(500, getTestSimpleList()...)
	assert.Equal(expected, testSimpleListFromNomsList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, cl6.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl6))

	expected = expected.Insert(600, Int64(100))
	assert.Equal(expected, testSimpleListFromNomsList(cl7))
	assert.Equal(3*getTestSimpleListLen()+5, cl7.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl7))
}

func TestCompoundListInsertRanges(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	testList := getTestSimpleList()
	whole := testList.ToNomsList(cs)

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			actual := testListPart.ToNomsList(cs).Insert(uint64(i), testList[i:i+window]...)
			assert.Equal(whole.Len(), actual.Len())
			assert.True(whole.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		assert.Equal(len(testList)+incr, int(whole.Insert(uint64(i), testList[0:incr]...).Len()))
	}
}

func TestCompoundListRemoveNothing(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	cl := getTestSimpleList().ToNomsList(cs)

	assert.True(cl.Equals(cl.Remove(0, 0)))
	for i := uint64(1); i < getTestSimpleListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Remove(i, i)))
	}
	assert.True(cl.Equals(cl.Remove(cl.Len()-1, cl.Len()-1)))
	assert.True(cl.Equals(cl.Remove(cl.Len(), cl.Len())))
}

func TestCompoundListRemoveAtMiddle(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	cl := getTestSimpleList().ToNomsList(cs)
	cl2 := cl.RemoveAt(100)
	cl3 := cl2.RemoveAt(200)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleListFromNomsList(cl))
	assert.Equal(getTestSimpleListLen(), cl.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl))

	expected = expected.RemoveAt(100)
	assert.Equal(expected, testSimpleListFromNomsList(cl2))
	assert.Equal(getTestSimpleListLen()-1, cl2.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl2))

	expected = expected.RemoveAt(200)
	assert.Equal(expected, testSimpleListFromNomsList(cl3))
	assert.Equal(getTestSimpleListLen()-2, cl3.Len())
	assert.True(expected.ToNomsList(cs).Equals(cl3))
}

func TestCompoundListRemoveRanges(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	testList := getTestSimpleList()
	whole := testList.ToNomsList(cs)

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			expected := testListPart.ToNomsList(cs)
			actual := whole.Remove(uint64(i), uint64(i+window))
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList)-incr; i += incr {
		assert.Equal(len(testList)-incr, int(whole.Remove(uint64(i), uint64(i+incr)).Len()))
	}
}

func TestCompoundListSet(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	testList := getTestSimpleList()
	cl := testList.ToNomsList(cs)

	testIdx := func(idx int, testEquality bool) {
		newVal := Int64(-1) // Test values are never < 0
		cl2 := cl.Set(uint64(idx), newVal)
		assert.False(cl.Equals(cl2))
		if testEquality {
			assert.True(testList.Set(idx, newVal).ToNomsList(cs).Equals(cl2))
		}
	}

	// Compare list equality. Increment by 100 because each iteration requires building a new list, which is slow, but always test the last index.
	for incr, i := 100, 0; i < len(testList); i += incr {
		testIdx(i, true)
	}
	testIdx(len(testList)-1, true)

	// Compare list unequality, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		testIdx(i, false)
	}
}
