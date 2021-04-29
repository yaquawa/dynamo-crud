import { Path } from 'ts-toolbelt/out/Object/Path'
import { Split } from 'ts-toolbelt/out/String/Split'
import { Optional } from 'ts-toolbelt/out/Object/Optional'

type BinaryTypes =
  | ArrayBuffer
  | Blob
  | DataView
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array

export type SetType = Set<number | string | BinaryTypes>

type KeyofTypeWhereValueIs<T, V> = { [K in keyof T]-?: T[K] extends V ? K : never }[keyof T]
export type PrimaryKeyValueType = string | number | BinaryTypes
export type PrimaryKeyNameCandidates<Model> = KeyofTypeWhereValueIs<Required<Model>, PrimaryKeyValueType>
export type PrimaryKeyCandidates<Model> = Partial<Pick<Model, PrimaryKeyNameCandidates<Model>>>

export type GetPrimaryKey<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never,
  RequireSK extends boolean = true
> = RequireSK extends true ? Pick<Model, PK | SK> : Optional<Pick<Model, PK | SK>, SK>

type ObjectType = { [x: string]: any }
type ArrayType = any[] | readonly any[]
type ExtractArrayType<T> = T extends Array<infer U> | ReadonlyArray<infer U> ? U : never

type _ExtractPathExpressions<T, TargetType> = Exclude<
  keyof {
    [P in Exclude<keyof T, symbol> as T[P] extends ArrayType
      ?
          | (T[P] extends TargetType ? P : never)
          | (ExtractArrayType<T[P]> extends TargetType ? `${P}.${number}` : never)
          | `${P}.${number}.${Exclude<
              _ExtractPathExpressions<T[P][number], TargetType>,
              keyof number | keyof string
            >}`
      : T[P] extends ObjectType
      ? `${P}.${_ExtractPathExpressions<T[P], TargetType>}` | (T[P] extends TargetType ? P : never)
      : T[P] extends TargetType
      ? P
      : never]: string
  },
  symbol
>

export type ExtractPathExpressions<T, TargetType = any> = _ExtractPathExpressions<Required<T>, TargetType>

export type GetTypeByPath<T, P> = P extends string ? Path<T, Split<P, '.'>> : never

export type UnpackPromise<T extends Promise<any>> = T extends Promise<infer U> ? U : never
