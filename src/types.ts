import { Optional } from 'ts-toolbelt/out/Object/Optional'
import { UpdatableQueryCommand } from './QueryCommand'
import { UpdatableScanCommand } from './ScanCommand'

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
          | (T[P] extends string | number | boolean
              ? never
              : `${P}.${number}.${_ExtractPathExpressions<T[P][number], TargetType>}`)
      : T[P] extends ObjectType
      ? `${P}.${_ExtractPathExpressions<T[P], TargetType>}` | (T[P] extends TargetType ? P : never)
      : T[P] extends TargetType
      ? P
      : never]: string
  },
  symbol
>

export type ExtractPathExpressions<T, TargetType = any> = _ExtractPathExpressions<Required<T>, TargetType>

type Idx<T, K> = Exclude<keyof T, symbol> extends infer KK
  ? KK extends keyof T
    ? K extends `${Exclude<KK, symbol>}`
      ? T[KK]
      : never
    : never
  : never

export type GetTypeByPath<T, K> = T extends object
  ? K extends `${infer F}.${infer R}`
    ? GetTypeByPath<Idx<T, F>, R>
    : Idx<T, K>
  : never

export type UnpackPromise<T extends Promise<any>> = T extends Promise<infer U> ? U : never

export type TokenBucket = { removeTokens(count: number): Promise<number> }

export type GetCommandModel<
  Command extends UpdatableQueryCommand<any, any, any> | UpdatableScanCommand<any>
> = Command extends UpdatableQueryCommand<infer Model, any, any>
  ? Model
  : Command extends UpdatableScanCommand<infer Model>
  ? Model
  : never
