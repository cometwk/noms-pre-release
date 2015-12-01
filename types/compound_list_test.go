package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSimpleList []Value

func (tsl testSimpleList) Get(idx uint64) Value {
	return tsl[idx]
}

func (tsl testSimpleList) Insert(idx uint64, vs ...Value) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, vs...)
	res = append(res, tsl[idx:]...)
	return
}

func (tsl testSimpleList) Remove(idx uint64) (res testSimpleList) {
	res = append(res, tsl[:idx]...)
	res = append(res, tsl[idx+1:]...)
	return
}

func getTestSimpleListLen() int {
	return int(listPattern * 16)
}

func getTestSimpleList() testSimpleList {
	length := getTestSimpleListLen()
	s := rand.NewSource(42)
	values := make([]Value, length)
	for i := 0; i < length; i++ {
		values[i] = Int64(s.Int63() & 0xff)
	}

	return values
}

func compoundFromTestSimpleList(items testSimpleList) compoundList {
	cs := chunks.NewMemoryStore()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	return NewCompoundList(tr, cs, items...).(compoundList)
}

func testSimpleFromCompoundList(cl compoundList) (simple testSimpleList) {
	cl.IterAll(func(v Value, offset uint64) {
		simple = append(simple, v)
	})
	return
}

func TestCompoundListGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	for i, v := range simpleList {
		assert.Equal(v, cl.Get(uint64(i)))
	}
}

func TestCompoundListIter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	expectIdx := uint64(0)
	endAt := uint64(listPattern)
	cl.Iter(func(v Value, idx uint64) bool {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
		if expectIdx == endAt {
			return true
		}
		return false
	})

	assert.Equal(endAt, expectIdx)
}

func TestCompoundListIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
	})
}

func TestCompoundListCurAt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	listLen := func(at int, next func(*sequenceCursor) bool) (size int) {
		cs := chunks.NewMemoryStore()
		tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
		cl := NewCompoundList(tr, cs, getTestSimpleList()...).(compoundList)
		cur, _, _ := cl.cursorAt(uint64(at))
		for {
			size += int(ReadValue(cur.current().(metaTuple).ref, cs).(List).Len())
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
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cl := compoundFromTestSimpleList(getTestSimpleList())
	cl2 := cl.Append(Int64(42))
	cl3 := cl2.Append(Int64(43))
	cl4 := cl3.Append(getTestSimpleList()...)
	cl5 := cl4.Append(Int64(44), Int64(45))
	cl6 := cl5.Append(getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleFromCompoundList(cl))
	assert.Equal(getTestSimpleListLen(), int(cl.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl))

	expected = append(expected, Int64(42))
	assert.Equal(expected, testSimpleFromCompoundList(cl2))
	assert.Equal(getTestSimpleListLen()+1, int(cl2.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl2))

	expected = append(expected, Int64(43))
	assert.Equal(expected, testSimpleFromCompoundList(cl3))
	assert.Equal(getTestSimpleListLen()+2, int(cl3.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl3))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, int(cl4.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl4))

	expected = append(expected, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleFromCompoundList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, int(cl5.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl5))

	expected = append(expected, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, int(cl6.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl6))
}

func TestCompoundListInsertStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cl := compoundFromTestSimpleList(getTestSimpleList())
	cl2 := cl.Insert(0, Int64(42))
	cl3 := cl2.Insert(0, Int64(43))
	cl4 := cl3.Insert(0, getTestSimpleList()...)
	cl5 := cl4.Insert(0, Int64(44), Int64(45))
	cl6 := cl5.Insert(0, getTestSimpleList()...)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleFromCompoundList(cl))
	assert.Equal(getTestSimpleListLen(), int(cl.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl))

	expected = expected.Insert(0, Int64(42))
	assert.Equal(expected, testSimpleFromCompoundList(cl2))
	assert.Equal(getTestSimpleListLen()+1, int(cl2.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl2))

	expected = expected.Insert(0, Int64(43))
	assert.Equal(expected, testSimpleFromCompoundList(cl3))
	assert.Equal(getTestSimpleListLen()+2, int(cl3.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl3))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, int(cl4.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl4))

	expected = expected.Insert(0, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleFromCompoundList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, int(cl5.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl5))

	expected = expected.Insert(0, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, int(cl6.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl6))
}

func TestCompoundListInsertMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cl := compoundFromTestSimpleList(getTestSimpleList())
	cl2 := cl.Insert(100, Int64(42))
	cl3 := cl2.Insert(200, Int64(43))
	cl4 := cl3.Insert(300, getTestSimpleList()...)
	cl5 := cl4.Insert(400, Int64(44), Int64(45))
	cl6 := cl5.Insert(500, getTestSimpleList()...)
	cl7 := cl6.Insert(600, Int64(100))

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleFromCompoundList(cl))
	assert.Equal(getTestSimpleListLen(), int(cl.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl))

	expected = expected.Insert(100, Int64(42))
	assert.Equal(expected, testSimpleFromCompoundList(cl2))
	assert.Equal(getTestSimpleListLen()+1, int(cl2.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl2))

	expected = expected.Insert(200, Int64(43))
	assert.Equal(expected, testSimpleFromCompoundList(cl3))
	assert.Equal(getTestSimpleListLen()+2, int(cl3.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl3))

	expected = expected.Insert(300, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl4))
	assert.Equal(2*getTestSimpleListLen()+2, int(cl4.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl4))

	expected = expected.Insert(400, Int64(44), Int64(45))
	assert.Equal(expected, testSimpleFromCompoundList(cl5))
	assert.Equal(2*getTestSimpleListLen()+4, int(cl5.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl5))

	expected = expected.Insert(500, getTestSimpleList()...)
	assert.Equal(expected, testSimpleFromCompoundList(cl6))
	assert.Equal(3*getTestSimpleListLen()+4, int(cl6.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl6))

	expected = expected.Insert(600, Int64(100))
	assert.Equal(expected, testSimpleFromCompoundList(cl7))
	assert.Equal(3*getTestSimpleListLen()+5, int(cl7.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl7))
}

func TestCompoundListRemoveMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cl := compoundFromTestSimpleList(getTestSimpleList())
	cl2 := cl.Remove(100)
	cl3 := cl2.Remove(200)

	expected := getTestSimpleList()
	assert.Equal(expected, testSimpleFromCompoundList(cl))
	assert.Equal(getTestSimpleListLen(), int(cl.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl))

	expected = expected.Remove(100)
	assert.Equal(expected, testSimpleFromCompoundList(cl2))
	assert.Equal(getTestSimpleListLen()-1, int(cl2.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl2))

	expected = expected.Remove(200)
	assert.Equal(expected, testSimpleFromCompoundList(cl3))
	assert.Equal(getTestSimpleListLen()-2, int(cl3.Len()))
	assert.True(compoundFromTestSimpleList(expected).Equals(cl3))
}

// TODO also test somehow the number of opeations (number of items dereferenced?) to make sure we're not just looping over the entire tree after all.
// TODO test the sequence chunker without ever calling append or skip on it. it should result in the same tree.
