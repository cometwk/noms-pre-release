// @flow

import {NomsBlob, BlobLeafSequence} from './blob.js';
import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct from './struct.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {decode as decodeBase64} from './base64.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType,
    makeUnresolvedType, StructDesc, Type, blobType} from './type.js';
import {indexTypeForMetaSequence, MetaTuple, newMetaSequenceFromData} from './meta_sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {lookupPackage, Package, readPackage} from './package.js';
import {NomsMap, MapLeafSequence} from './map.js';
import {setDecodeNomsValue} from './read_value.js';
import {NomsSet, SetLeafSequence} from './set.js';

const typedTag = 't ';
const blobTag = 'b ';

export function arrayBufferToBlob(cs: ChunkStore, t: Type, buf: ArrayBuffer): NomsBlob {
  // TODO: Change Sequence so that we don't need to convert to an Array.
  const items = [];
  for (let arr = new Uint8Array(buf), i = 0; i < arr.length; i++) {
    items.push(arr[i]);
  }
  return new NomsBlob(t, new BlobLeafSequence(cs, t, items));
}

class UnresolvedPackage {
  pkgRef: Ref;

  constructor(pkgRef: Ref) {
    this.pkgRef = pkgRef;
  }
}

class JsonArrayReader {
  _a: Array<any>;
  _i: number;
  _cs: ChunkStore;

  constructor(a: Array<any>, cs: ChunkStore) {
    this._a = a;
    this._i = 0;
    this._cs = cs;
  }

  read(): any {
    return this._a[this._i++];
  }

  atEnd(): boolean {
    return this._i >= this._a.length;
  }

  readString(): string {
    const next = this.read();
    invariant(typeof next === 'string');
    return next;
  }

  readBool(): boolean {
    const next = this.read();
    invariant(typeof next === 'boolean');
    return next;
  }

  readInt(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseInt(next, 10);
  }

  readUint(): number {
    const v = this.readInt();
    invariant(v >= 0);
    return v;
  }

  readFloat(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseFloat(next);
  }

  readOrdinal(): number {
    return this.readInt();
  }

  readArray(): Array<any> {
    const next = this.read();
    invariant(Array.isArray(next));
    return next;
  }

  readKind(): NomsKind {
    const next = this.read();
    invariant(typeof next === 'number');
    return next;
  }

  readRef(): Ref {
    const next = this.readString();
    return Ref.parse(next);
  }

  readTypeAsTag(): Type {
    const kind = this.readKind();
    switch (kind) {
      case Kind.List:
      case Kind.Set:
      case Kind.Ref: {
        const elemType = this.readTypeAsTag();
        return makeCompoundType(kind, elemType);
      }
      case Kind.Map: {
        const keyType = this.readTypeAsTag();
        const valueType = this.readTypeAsTag();
        return makeCompoundType(kind, keyType, valueType);
      }
      case Kind.Type:
        return makePrimitiveType(Kind.Type);
      case Kind.Unresolved: {
        const pkgRef = this.readRef();
        const ordinal = this.readOrdinal();
        return makeType(pkgRef, ordinal);
      }
    }

    if (isPrimitiveKind(kind)) {
      return makePrimitiveType(kind);
    }

    throw new Error('Unreachable');
  }

  readBlob(t: Type): NomsBlob {
    return arrayBufferToBlob(this._cs, t, decodeBase64(this.readString()));
  }

  readSequence(t: Type, pkg: ?Package): Array<any> {
    const elemType = t.elemTypes[0];
    const list = [];
    while (!this.atEnd()) {
      const v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type, pkg: ?Package): ListLeafSequence {
    const seq = this.readSequence(t, pkg);
    return new ListLeafSequence(this._cs, t, seq);
  }

  readSetLeafSequence(t: Type, pkg: ?Package): SetLeafSequence {
    const seq = this.readSequence(t, pkg);
    return new SetLeafSequence(this._cs, t, seq);
  }

  readMapLeafSequence(t: Type, pkg: ?Package): MapLeafSequence {
    const keyType = t.elemTypes[0];
    const valueType = t.elemTypes[1];
    const entries = [];
    while (!this.atEnd()) {
      const k = this.readValueWithoutTag(keyType, pkg);
      const v = this.readValueWithoutTag(valueType, pkg);
      entries.push({key: k, value: v});
    }

    return new MapLeafSequence(this._cs, t, entries);
  }

  readEnum(): number {
    return this.readUint();
  }

  readMetaSequence(t: Type, pkg: ?Package): any {
    const data: Array<MetaTuple> = [];
    const indexType = indexTypeForMetaSequence(t);
    while (!this.atEnd()) {
      const ref = this.readRef();
      const v = this.readValueWithoutTag(indexType, pkg);
      data.push(new MetaTuple(ref, v));
    }

    return newMetaSequenceFromData(this._cs, t, data);
  }

  readPackage(t: Type, pkg: ?Package): Package {
    const r2 = new JsonArrayReader(this.readArray(), this._cs);
    const types = [];
    while (!r2.atEnd()) {
      types.push(r2.readTypeAsValue(pkg));
    }

    const r3 = new JsonArrayReader(this.readArray(), this._cs);
    const deps = [];
    while (!r3.atEnd()) {
      deps.push(r3.readRef());
    }

    return new Package(types, deps);
  }

  readTopLevelValue(): Promise<any> {
    return new Promise((resolve, reject) => {
      const t = this.readTypeAsTag();
      const doRead = () => {
        const i = this._i;

        try {
          const v = this.readValueWithoutTag(t);
          resolve(v);
        } catch (ex) {
          if (ex instanceof UnresolvedPackage) {
            readPackage(ex.pkgRef, this._cs).then(() => {
              this._i = i;
              doRead();
            });
          } else {
            reject(ex);
          }
        }
      };

      doRead();
    });
  }

  readValueWithoutTag(t: Type, pkg: ?Package = null): any {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this.readBool();
        if (isMeta) {
          const r2 = new JsonArrayReader(this.readArray(), this._cs);
          return r2.readMetaSequence(t, pkg);
        }
        return this.readBlob(t);
      }
      case Kind.Bool:
        return this.readBool();
      case Kind.Float32:
      case Kind.Float64:
        return this.readFloat();
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
        return this.readInt();
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
        return this.readUint();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        const t2 = this.readTypeAsTag();
        return this.readValueWithoutTag(t2, pkg);
      }
      case Kind.List: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._cs);
        const sequence = isMeta ?
            r2.readMetaSequence(t, pkg) :
            r2.readListLeafSequence(t, pkg);
        return new NomsList(t, sequence);
      }
      case Kind.Map: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._cs);
        const sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readMapLeafSequence(t, pkg);
        return new NomsMap(t, sequence);
      }
      case Kind.Package:
        return this.readPackage(t, pkg);
      case Kind.Ref:
        // TODO: This is not aligned with Go. In Go we have a dedicated Value
        // for refs.
        return this.readRef();
      case Kind.Set: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._cs);
        const sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readSetLeafSequence(t, pkg);
        return new NomsSet(t, sequence);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.Type:
        return this.readTypeAsValue(pkg);
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  readUnresolvedKindToValue(t: Type, pkg: ?Package = null): any {
    const pkgRef = t.packageRef;
    const ordinal = t.ordinal;
    if (!pkgRef.isEmpty()) {
      pkg = lookupPackage(pkgRef);
      if (!pkg) {
        throw new UnresolvedPackage(pkgRef);
      }
      invariant(pkg);
    }

    pkg = notNull(pkg);
    const typeDef = pkg.types[ordinal];
    if (typeDef.kind === Kind.Enum) {
      return this.readEnum();
    }

    invariant(typeDef.kind === Kind.Struct);
    return this.readStruct(typeDef, t, pkg);
  }

  readTypeAsValue(pkg: ?Package): Type {
    const k = this.readKind();

    switch (k) {
      case Kind.Enum:
        const name = this.readString();
        const r2 = new JsonArrayReader(this.readArray(), this._cs);
        const ids = [];
        while (!r2.atEnd()) {
          ids.push(r2.readString());
        }
        return makeEnumType(name, ids);
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        const r2 = new JsonArrayReader(this.readArray(), this._cs);
        const elemTypes: Array<Type> = [];
        while (!r2.atEnd()) {
          elemTypes.push(r2.readTypeAsValue());
        }

        return makeCompoundType(k, ...elemTypes);
      }
      case Kind.Struct: {
        const name = this.readString();
        const readFields = () => {
          const fields: Array<Field> = [];
          const fieldReader = new JsonArrayReader(this.readArray(), this._cs);
          while (!fieldReader.atEnd()) {
            const fieldName = fieldReader.readString();
            const fieldType = fieldReader.readTypeAsValue(pkg);
            const optional = fieldReader.readBool();
            fields.push(new Field(fieldName, fieldType, optional));
          }
          return fields;
        };

        const fields = readFields();
        const choices = readFields();
        return makeStructType(name, fields, choices);
      }
      case Kind.Unresolved: {
        const pkgRef = this.readRef();
        const ordinal = this.readOrdinal();
        if (ordinal === -1) {
          const namespace = this.readString();
          const name = this.readString();
          return makeUnresolvedType(namespace, name);
        }

        return makeType(pkgRef, ordinal);
      }
    }

    invariant(isPrimitiveKind(k));
    return makePrimitiveType(k);

  }

  readStruct(typeDef: Type, type: Type, pkg: Package): Struct {
    // TODO FixupType?
    const desc = typeDef.desc;
    invariant(desc instanceof StructDesc);

    const s: { [key: string]: any } = Object.create(null);

    for (let i = 0; i < desc.fields.length; i++) {
      const field = desc.fields[i];
      if (field.optional) {
        const b = this.readBool();
        if (b) {
          const v = this.readValueWithoutTag(field.t, pkg);
          s[field.name] = v;
        }
      } else {
        const v = this.readValueWithoutTag(field.t, pkg);
        s[field.name] = v;
      }
    }

    let unionIndex = -1;
    if (desc.union.length > 0) {
      unionIndex = this.readUint();
      const unionField = desc.union[unionIndex];
      const v = this.readValueWithoutTag(unionField.t, pkg);
      s[unionField.name] = v;
    }

    return new Struct(type, typeDef, s);
  }
}

function decodeNomsValue(chunk: Chunk, cs: ChunkStore): Promise<any> {
  const tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      const payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      const reader = new JsonArrayReader(payload, cs);
      return reader.readTopLevelValue();
    }
    case blobTag:
      return Promise.resolve(arrayBufferToBlob(cs, blobType, chunk.data.buffer.slice(2)));
    default:
      throw new Error('Not implemented');
  }
}

export {decodeNomsValue, indexTypeForMetaSequence, JsonArrayReader};

setDecodeNomsValue(decodeNomsValue); // TODO: Avoid cyclic badness with commonjs.
