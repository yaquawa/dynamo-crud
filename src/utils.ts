import { UpdatableQueryCommand } from './QueryCommand'
import { UpdatableScanCommand } from './ScanCommand'
import { GetCommandModel } from './types'

export function omit<T extends Record<string, any>, Key extends keyof T>(obj: T, keys: Key[]): Omit<T, Key> {
  const cloneObj = { ...obj }

  for (const key of keys) {
    if (key in obj) {
      delete cloneObj[key]
    }
  }

  return cloneObj
}

export function pick<T extends Record<string, any>, Key extends keyof T>(obj: T, keys: Key[]): Pick<T, Key> {
  return keys.reduce<Pick<T, Key>>((pickObj, key) => {
    if (key in obj) {
      pickObj[key] = obj[key]
    }

    return pickObj
  }, {} as any)
}

export function isEmpty(value: any): boolean {
  if (typeof value === 'object') return Object.keys(value).length === 0

  return !!value
}

export function getBinarySize(data: Record<string, any>): number {
  return Buffer.byteLength(JSON.stringify(data), 'utf8')
}

export function chunk<T extends Record<string, any>>({
  data,
  maxBytesPerItem,
  maxLength,
}: {
  data: T[]
  maxBytesPerItem: number
  maxLength: number
}): T[][] {
  let result: T[][] = []
  let chunk: T[] = []

  for (const item of data) {
    const binarySize = getBinarySize(item)

    if (binarySize > maxBytesPerItem) {
      throw new Error(
        `[dynamo-crud] Exceeds 'maxBytesPerItem', couldn't chunk object ${JSON.stringify(item)}`
      )
    }

    if (chunk.length === maxLength) {
      result.push(chunk)
      chunk = [item]
      continue
    }

    chunk.push(item)
  }

  if (chunk.length) {
    result.push(chunk)
  }

  return result
}

/**
 * `Promise.all` will gather the resolved values which may cost unnecessary memory.
 * Use this one if you're not going to use the gathered resolved values.
 * @param promises
 */
export function runParallel(promises: Promise<any>[]): Promise<void> {
  return new Promise((resolve, reject) => {
    let remainingCount = promises.length

    for (const promise of promises) {
      promise
        .then(() => {
          if (--remainingCount === 0) {
            resolve()
          }
        })
        .catch((error) => {
          reject(error)
        })
    }
  })
}

export async function consume<
  Command extends UpdatableQueryCommand<any, any, any> | UpdatableScanCommand<any>
>(
  consumer: (model: GetCommandModel<Command>) => void | null | Promise<void | null>,
  command: Command
): Promise<void> {
  for await (const { data: models } of command.createReadableStream()) {
    if (!models) continue

    for (const model of models) {
      const result = await consumer(model)
      if (result === null) return
    }
  }
}

export async function getOneItem<
  Command extends UpdatableQueryCommand<any, any, any> | UpdatableScanCommand<any>
>(command: Command): Promise<GetCommandModel<Command> | null> {
  let result: GetCommandModel<Command> | null = null

  command.limit(1)

  await consume((model) => {
    result = model
    return null
  }, command)

  return result
}
