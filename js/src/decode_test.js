// @flow

import {encode as encodeBase64} from './base64.js';
import {NomsBlob} from './blob.js';
import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import test from './async_test.js';
import type {float64, int32, int64, uint8, uint16, uint32, uint64} from './primitives.js';
import type {TypeDesc} from './type.js';
import {assert} from 'chai';
import {arrayBufferToBlob, decodeNomsValue, JsonArrayReader} from './decode.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, Type,
    blobType} from './type.js';
import {IndexedMetaSequence, MetaTuple} from './meta_sequence.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms_kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {readValue} from './read_value.js';
import {registerPackage, Package} from './package.js';
import {suite} from 'mocha';
import {Value} from './value.js';
import {writeValue} from './encode.js';

suite('Decode', () => {
  function stringToBuffer(s) {
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
      bytes[i] = s.charCodeAt(i);
    }
    return bytes.buffer;
  }

  test('read', async () => {
    const ms = new MemoryStore();
    const a = [1, 'hi', true];
    const r = new JsonArrayReader(a, ms);

    assert.strictEqual(1, r.read());
    assert.isFalse(r.atEnd());

    assert.strictEqual('hi', r.readString());
    assert.isFalse(r.atEnd());

    assert.strictEqual(true, r.readBool());
    assert.isTrue(r.atEnd());
  });

  test('read type as tag', async () => {
    const ms = new MemoryStore();

    function doTest(expected: Type, a: Array<any>) {
      const r = new JsonArrayReader(a, ms);
      const tr = r.readTypeAsTag();
      assert.isTrue(expected.equals(tr));
    }

    doTest(makePrimitiveType(Kind.Bool), [Kind.Bool, true]);
    doTest(makePrimitiveType(Kind.Type), [Kind.Type, Kind.Bool]);
    doTest(makeCompoundType(Kind.List, makePrimitiveType(Kind.Bool)),
                            [Kind.List, Kind.Bool, true, false]);

    const pkgRef = Ref.parse('sha1-a9993e364706816aba3e25717850c26c9cd0d89d');
    doTest(makeType(pkgRef, 42), [Kind.Unresolved, pkgRef.toString(), '42']);

    doTest(makePrimitiveType(Kind.Type), [Kind.Type, Kind.Type, pkgRef.toString()]);
  });

  test('read primitives', async () => {
    const ms = new MemoryStore();

    async function doTest(expected: any, a: Array<any>): Promise<void> {
      const r = new JsonArrayReader(a, ms);
      const v = await r.readTopLevelValue();
      assert.deepEqual(expected, v);
    }

    await doTest(true, [Kind.Bool, true]);
    await doTest(false, [Kind.Bool, false]);
    await doTest(0, [Kind.Uint8, '0']);
    await doTest(0, [Kind.Uint16, '0']);
    await doTest(0, [Kind.Uint32, '0']);
    await doTest(0, [Kind.Uint64, '0']);
    await doTest(0, [Kind.Int8, '0']);
    await doTest(0, [Kind.Int16, '0']);
    await doTest(0, [Kind.Int32, '0']);
    await doTest(0, [Kind.Int64, '0']);
    await doTest(0, [Kind.Float32, '0']);
    await doTest(0, [Kind.Float64, '0']);

    await doTest(1e18, [Kind.Int64, '1000000000000000000']);
    await doTest(1e19, [Kind.Uint64, '10000000000000000000']);
    await doTest(1e19, [Kind.Float64, '10000000000000000000']);
    await doTest(1e20, [Kind.Float64, '1e+20']);

    await doTest('hi', [Kind.String, 'hi']);
  });

  test('read list of int 32', async () => {
    const ms = new MemoryStore();
    const a = [Kind.List, Kind.Int32, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsList<int32> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const l = new NomsList(tr, new ListLeafSequence(ms, tr, [0, 1, 2, 3]));
    assert.isTrue(l.equals(v));
  });

  // TODO: Can't round-trip collections of value types. =-(
  test('read list of value', async () => {
    const ms = new MemoryStore();
    const a = [Kind.List, Kind.Value, false, [Kind.Int32, '1', Kind.String, 'hi', Kind.Bool, true]];
    const r = new JsonArrayReader(a, ms);
    const v:NomsList<Value> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Value));
    assert.isTrue(v.type.equals(tr));
    assert.strictEqual(1, await v.get(0));
    assert.strictEqual('hi', await v.get(1));
    assert.strictEqual(true, await v.get(2));
  });

  test('read value list of int8', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Value, Kind.List, Kind.Int8, false, ['0', '1', '2']];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int8));
    const l = new NomsList(tr, new ListLeafSequence(ms, tr, [0, 1, 2]));
    assert.isTrue(l.equals(v));
  });

  test('read compound list', async () => {
    const ms = new MemoryStore();

    const ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const r1 = writeValue(new NomsList(ltr, new ListLeafSequence(ms, ltr, [0, 1])), ltr, ms);
    const r2 = writeValue(new NomsList(ltr, new ListLeafSequence(ms, ltr, [2, 3])), ltr, ms);
    const r3 = writeValue(new NomsList(ltr, new ListLeafSequence(ms, ltr, [4, 5])), ltr, ms);
    const tuples = [
      new MetaTuple(r1, 2),
      new MetaTuple(r2, 4),
      new MetaTuple(r3, 6),
    ];
    const l:NomsList<int32> = new NomsList(ltr, new IndexedMetaSequence(ms, ltr, tuples));
    invariant(l instanceof NomsList);

    const a = [Kind.List, Kind.Int32, true,
               [r1.toString(), '2', r2.toString(), '4', r3.toString(), '6']];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();
    invariant(v instanceof NomsList);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read map of int64 to float64', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Map, Kind.Int64, Kind.Float64, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsMap<int64, float64> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const t = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                               makePrimitiveType(Kind.Float64));
    const m = new NomsMap(t, new MapLeafSequence(ms, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read map of ref to uint64', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Map, Kind.Ref, Kind.Value, Kind.Uint64, false,
               ['sha1-0000000000000000000000000000000000000001', '2',
                'sha1-0000000000000000000000000000000000000002', '4']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsMap<Ref, uint64> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const t = makeCompoundType(Kind.Map, makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Value)),
                               makePrimitiveType(Kind.Uint64));
    const m = new NomsMap(t,
        new MapLeafSequence(ms, t, [{key: new Ref('sha1-0000000000000000000000000000000000000001'),
            value: 2}, {key: new Ref('sha1-0000000000000000000000000000000000000002'), value: 4}]));
    assert.isTrue(v.equals(m));
  });

  test('read value map of uint64 to uint32', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Value, Kind.Map, Kind.Uint64, Kind.Uint32, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsMap<uint64, uint32> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const t = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Uint64),
                               makePrimitiveType(Kind.Uint32));
    const m = new NomsMap(t, new MapLeafSequence(ms, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read set of uint8', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Set, Kind.Uint8, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsSet<uint8> = await r.readTopLevelValue();
    invariant(v instanceof NomsSet);

    const t = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Uint8));
    const s = new NomsSet(t, new SetLeafSequence(ms, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  test('read value set of uint16', async () => {
    const ms = new MemoryStore();
    const a = [Kind.Value, Kind.Set, Kind.Uint16, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ms);
    const v:NomsSet<uint16> = await r.readTopLevelValue();
    invariant(v instanceof NomsSet);

    const t = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Uint16));
    const s = new NomsSet(t, new SetLeafSequence(ms, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  function assertStruct(s: ?Struct, desc: TypeDesc, data: {[key: string]: any}) {
    notNull(s);
    invariant(s instanceof Struct, 'expected instanceof struct');
    assert.deepEqual(desc, s.desc);

    for (const key in data) {
      assert.deepEqual(data[key], s.get(key));
    }
  }

  test('test read struct', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('s', makePrimitiveType(Kind.String), false),
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', '42', 'hi', true];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read struct union', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A2', [
      new Field('x', makePrimitiveType(Kind.Float32), false),
    ], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ]);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', '42', '1', 'hi'];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
    });
  });

  test('test read struct optional', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A3', [
      new Field('x', makePrimitiveType(Kind.Float32), false),
      new Field('s', makePrimitiveType(Kind.String), true),
      new Field('b', makePrimitiveType(Kind.Bool), true),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', '42', false, true, false];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      b: false,
    });
  });

  test('test read struct with list', async () => {
    const ms = new MemoryStore();

    const ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const tr = makeStructType('A4', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('l', ltr, false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', true, false, ['0', '1', '2'], 'hi'];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      l: new NomsList(ltr, new ListLeafSequence(ms, ltr, [0, 1, 2])),
      s: 'hi',
    });
  });

  test('test read struct with value', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A5', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('v', makePrimitiveType(Kind.Value), false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', true, Kind.Uint8, '42', 'hi'];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      v: 42,
      s: 'hi',
    });
  });

  test('test read value struct', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('s', makePrimitiveType(Kind.String), false),
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Value, Kind.Unresolved, pkg.ref.toString(), '0', '42', 'hi', true];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read enum', async () => {
    const ms = new MemoryStore();

    const tr = makeEnumType('E', ['a', 'b', 'c']);
    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', '1'];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assert.deepEqual(1, v);
  });

  test('test read value enum', async () => {
    const ms = new MemoryStore();

    const tr = makeEnumType('E', ['a', 'b', 'c']);
    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Value, Kind.Unresolved, pkg.ref.toString(), '0', '1'];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assert.deepEqual(1, v);
  });

  test('test read struct with', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('e', makeType(new Ref(), 1), false),
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);
    const enumTref = makeEnumType('E', ['a', 'b', 'c']);
    const pkg = new Package([tr, enumTref], []);
    registerPackage(pkg);

    const a = [Kind.Unresolved, pkg.ref.toString(), '0', '42', '1', true];
    const r = new JsonArrayReader(a, ms);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      e: 1,
      b: true,
    });
  });

  test('test read map of string to struct', async () => {
    const ms = new MemoryStore();
    const tr = makeStructType('s', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('i', makePrimitiveType(Kind.Int32), false),
    ], []);

    const pkg = new Package([tr], []);
    registerPackage(pkg);

    const a = [Kind.Value, Kind.Map, Kind.String, Kind.Unresolved, pkg.ref.toString(), '0', false,
        ['bar', false, '2', 'baz', false, '1', 'foo', true, '3']];

    const r = new JsonArrayReader(a, ms);
    const v:NomsMap<string, Struct> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    assert.strictEqual(3, v.size);
    assertStruct(await v.get('foo'), tr.desc, {b: true, i: 3});
    assertStruct(await v.get('bar'), tr.desc, {b: false, i: 2});
    assertStruct(await v.get('baz'), tr.desc, {b: false, i: 1});
  });

  test('decodeNomsValue', async () => {
    const ms = new MemoryStore();
    const chunk = Chunk.fromString(
        `t [${Kind.Value}, ${Kind.Set}, ${Kind.Uint16}, false, ["0", "1", "2", "3"]]`);
    const v:NomsSet<uint16> = await decodeNomsValue(chunk, new MemoryStore());

    const t = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Uint16));
    const s:NomsSet<uint16> = new NomsSet(t, new SetLeafSequence(ms, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  test('decodeNomsValue: counter with one commit', async () => {
    const ms = new MemoryStore();
    const root = Ref.parse('sha1-c3680a063b73ac42c3075110108a48a91007abf7');
    ms.put(Chunk.fromString('t [15,11,16,21,"sha1-7546d804d845125bc42669c7a4c3f3fb909eca29","0",' +
        'false,["counter","sha1-a6fffab4e12b49d57f194f0d3add9f6623a13e19"]]')); // root
    ms.put(Chunk.fromString('t [22,[19,"Commit",["value",13,false,"parents",17,[16,[21,' +
        '"sha1-0000000000000000000000000000000000000000","0"]],false],[]],[]]')); // datas package
    ms.put(Chunk.fromString('t [21,"sha1-4da2f91cdbba5a7c91b383091da45e55e16d2152","0",4,"1",' +
        'false,[]]')); // commit

    const rootMap = await readValue(root, ms);
    const counterRef = await rootMap.get('counter');
    const commit = await readValue(counterRef, ms);
    assert.strictEqual(1, await commit.get('value'));
  });

  test('out of line blob', async () => {
    const chunk = Chunk.fromString('b hi');
    const blob = await decodeNomsValue(chunk, new MemoryStore());
    const r = await blob.getReader().read();
    assert.isFalse(r.done);
    assert.equal(2, r.value.byteLength);
    assert.deepEqual(stringToBuffer('hi'), r.value);

    const data = new Uint8Array(2 + 256);
    data[0] = 'b'.charCodeAt(0);
    data[1] = ' '.charCodeAt(0);
    const bytes = new Uint8Array(256);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
      data[2 + i] = i;
    }

    const chunk2 = new Chunk(data);
    const blob2 = await decodeNomsValue(chunk2, new MemoryStore());
    const r2 = await blob2.getReader().read();
    assert.isFalse(r2.done);
    assert.equal(bytes.buffer.byteLength, r2.value.byteLength);
    assert.deepEqual(bytes.buffer, r2.value);
  });

  test('inline blob', async () => {
    const ms = new MemoryStore();
    const a = [
      Kind.List, Kind.Blob, false,
      [false, encodeBase64(stringToBuffer('hello')), false, encodeBase64(stringToBuffer('world'))],
    ];
    const r = new JsonArrayReader(a, ms);
    const v: NomsList<NomsBlob> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    assert.strictEqual(2, v.length);
    const [b1, b2] = [await v.get(0), await v.get(1)];
    assert.deepEqual({done: false, value: stringToBuffer('hello')}, await b1.getReader().read());
    assert.deepEqual({done: false, value: stringToBuffer('world')}, await b2.getReader().read());
  });

  test('compound blob', async () => {
    const ms = new MemoryStore();

    const r1 = writeValue(arrayBufferToBlob(ms, blobType, stringToBuffer('hi')), blobType, ms);
    const r2 = writeValue(arrayBufferToBlob(ms, blobType, stringToBuffer('world')), blobType, ms);

    const a = [Kind.Blob, true, [r1.ref, '2', r2.ref, '5']];
    const r = new JsonArrayReader(a, ms);
    const v: NomsBlob = await r.readTopLevelValue();
    invariant(v instanceof NomsBlob);

    const reader = v.getReader();
    assert.deepEqual({done: false, value: stringToBuffer('hi')}, await reader.read());
    assert.deepEqual({done: false, value: stringToBuffer('world')}, await reader.read());
    assert.deepEqual({done: true}, reader.read());
  });
});
