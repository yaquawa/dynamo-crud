import { chunk, omit, pick } from './utils'
import { ScanCommand } from './ScanCommand'
import { QueryCommand } from './QueryCommand'
import { CommandResult } from './CommandResult'
import { BatchWriteItemCommand } from './BatchWriteItemCommand'
import { PrimaryKeyNameCandidates, UnpackPromise } from './types'
import { BatchUpdateItemCommand, Updatable } from './UpdateItemCommand'
import { Readable, Writable, Duplex, ReadableOptions, WritableOptions, DuplexOptions } from 'stream'
import {
  UpdateItemCommandOutput,
  BatchWriteItemCommandOutput,
  WriteRequest,
  KeysAndAttributes,
  BatchGetItemCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { BatchGetItemCommand } from './BatchGetItemCommand'

export class ReadItemsCommandReadableStream<
  Command extends QueryCommand<any, any, any> | ScanCommand<any>
> extends Readable {
  private command: Command

  constructor(options: ReadableOptions & { command: Command }) {
    super({
      ...omit(options, ['command']),
      objectMode: true,
    })

    this.command = options.command
  }

  _read() {
    this.command._run().then(async (result) => {
      const { data, rawResponse } = result

      if (data || rawResponse.LastEvaluatedKey) {
        if (data) {
          if (this.command.tokenBucket) {
            const consumedCapacityUnits = result.rawResponse!.ConsumedCapacity!.CapacityUnits as number
            await this.command.tokenBucket.removeTokens(consumedCapacityUnits)
          }
          this.push(result)
        }

        if (rawResponse.LastEvaluatedKey) {
          this.command.setCommandInput({ ExclusiveStartKey: rawResponse.LastEvaluatedKey })
        } else {
          this.finishPush()
        }
      } else {
        this.finishPush()
      }
    })
  }

  private finishPush() {
    this.command.setCommandInput({ ExclusiveStartKey: undefined })
    this.push(null)
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<UnpackPromise<ReturnType<Command['_run']>>> {
    return super[Symbol.asyncIterator]()
  }
}

export class UpdateItemsCommandReadableStream<
  Model extends Record<string, any>,
  Command extends Updatable<Model, QueryCommand<Model, any, any> | ScanCommand<Model>>
> extends Readable {
  private readonly getRunGenerator: AsyncGenerator<
    CommandResult<undefined, undefined> | CommandResult<undefined, UpdateItemCommandOutput[]>,
    void,
    unknown
  >

  constructor(options: ReadableOptions & { command: Command; shouldReturnValue?: boolean }) {
    super({
      ...omit(options, ['command', 'shouldReturnValue']),
      objectMode: true,
    })

    this.getRunGenerator = options.command.getRunGenerator({ shouldReturnValue: options.shouldReturnValue })
  }

  _read() {
    this.readFromCommandResult()
  }

  readFromCommandResult() {
    this.getRunGenerator.next().then((result) => {
      if (result.done) {
        this.push(null)
        return
      }

      this.push(result.value)
    })
  }

  async consume(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.on('readable', () => {
        while (this.read() !== null) {}
      })

      this.on('end', resolve)
    })
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<
    CommandResult<undefined, undefined> | CommandResult<undefined, UpdateItemCommandOutput[]>
  > {
    return super[Symbol.asyncIterator]()
  }
}

export class UpdateItemsWritableStream<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> | undefined,
  Command extends BatchUpdateItemCommand<Model>,
  Chunk extends Model[]
> extends Writable {
  private command: Command
  private partitionKey: PK
  private sortKey?: SK

  constructor(options: WritableOptions & { command: Command; partitionKey: PK; sortKey?: SK }) {
    super({
      ...omit(options, ['command', 'partitionKey', 'sortKey']),
      objectMode: true,
    })

    this.command = options.command
    this.partitionKey = options.partitionKey
    this.sortKey = options.sortKey
  }

  _write(chunk: Chunk, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    const { partitionKey, sortKey } = this
    const primaryKeys = chunk.map((model) => {
      return pick(model, [partitionKey, ...(sortKey ? [sortKey as keyof Model] : [])])
    })

    this.command
      .run(primaryKeys)
      .then(() => {
        callback(null)
      })
      .catch((error: Error) => {
        callback(error)
      })
  }
}

export class BatchWriteItemCommandStream<Model extends Record<string, any>> extends Duplex {
  private writeRequestsChunks: WriteRequest[][] = []
  private readonly command: BatchWriteItemCommand<Model>
  private remainingRequestsCount = 0

  constructor(
    options: DuplexOptions & {
      command: BatchWriteItemCommand<Model>
    }
  ) {
    super({
      ...omit(options, ['command']),
      objectMode: true,
    })

    this.command = options.command
    this.appendChunks(options.command.getWriteRequests())
  }

  private appendChunks(writeRequests: WriteRequest[]) {
    const chunks = chunk({
      data: writeRequests,
      maxBytesPerItem: 400 * 1024,
      maxLength: 25,
    })

    this.writeRequestsChunks = this.writeRequestsChunks.concat(chunks)
    this.remainingRequestsCount += chunks.length
  }

  _write(chunk: WriteRequest[], _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    this.runCommand(chunk)
      .then(() => {
        callback()
      })
      .catch((error: Error) => {
        callback(error)
      })
  }

  _read() {
    const writeRequests = this.writeRequestsChunks.shift()

    if (!writeRequests) {
      return
    }

    this.readFromCommandResult(writeRequests)
  }

  private readFromCommandResult(writeRequests: WriteRequest[]) {
    this.runCommand(writeRequests)
      .then(async (result) => {
        this.push(result)

        if (this.remainingRequestsCount === 0) {
          this.push(null)
        }
      })
      .catch((error: Error) => {
        this.destroy(error)
      })
  }

  private async runCommand(writeRequests: WriteRequest[]) {
    const result = await this.command._run({ writeRequests })

    this.remainingRequestsCount--
    this.handleUnprocessedItems(result)

    if (this.remainingRequestsCount === 0) {
      return result
    }

    if (this.command.tokenBucket) {
      const consumedCapacityUnits = result.rawResponse!.ConsumedCapacity!.reduce<number>(
        (capacityUnits, consumedCapacity) => {
          const { CapacityUnits } = consumedCapacity

          if (CapacityUnits) {
            return capacityUnits + CapacityUnits
          }

          return capacityUnits
        },
        0
      )

      await this.command.tokenBucket.removeTokens(consumedCapacityUnits)
    }

    return result
  }

  private handleUnprocessedItems(result: CommandResult<undefined, BatchWriteItemCommandOutput>) {
    const unprocessedItems =
      result.rawResponse.UnprocessedItems && Object.values(result.rawResponse.UnprocessedItems)[0]

    if (unprocessedItems) {
      console.log(`Retry unprocessed items: ${JSON.stringify(unprocessedItems)}`)
      this.appendChunks(unprocessedItems)
    }
  }

  async consume(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.on('readable', () => {
        while (this.read() !== null) {}
      })

      this.on('end', resolve)
    })
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<CommandResult<undefined, BatchWriteItemCommandOutput>> {
    return super[Symbol.asyncIterator]()
  }
}

export class BatchGetItemCommandStream<Model extends Record<string, any>> extends Duplex {
  private requestsChunks: KeysAndAttributes[] = []
  private readonly command: BatchGetItemCommand<Model>
  private remainingRequestsCount = 0

  constructor(
    options: DuplexOptions & {
      command: BatchGetItemCommand<Model>
    }
  ) {
    super({
      ...omit(options, ['command']),
      objectMode: true,
    })

    this.command = options.command
    this.appendChunks(options.command.getKeysAndAttributes())
  }

  private appendChunks(keysAndAttributes: KeysAndAttributes) {
    const options = omit(keysAndAttributes, ['Keys'])
    const chunks = chunk({
      data: keysAndAttributes.Keys as NonNullable<typeof keysAndAttributes.Keys>,
      maxBytesPerItem: 400 * 1024,
      maxLength: 100,
    }).map((Keys) => {
      return {
        Keys,
        ...options,
      }
    })

    this.requestsChunks = this.requestsChunks.concat(chunks)
    this.remainingRequestsCount += chunks.length
  }

  _write(chunk: KeysAndAttributes, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    this.runCommand(chunk)
      .then(() => {
        callback()
      })
      .catch((error: Error) => {
        callback(error)
      })
  }

  _read() {
    const requests = this.requestsChunks.shift()

    if (!requests) {
      return
    }

    this.readFromCommandResult(requests)
  }

  private readFromCommandResult(requests: KeysAndAttributes) {
    this.runCommand(requests)
      .then(async (result) => {
        this.push(result)

        if (this.remainingRequestsCount === 0) {
          this.push(null)
        }
      })
      .catch((error: Error) => {
        this.destroy(error)
      })
  }

  private async runCommand(keysAndAttributes: KeysAndAttributes) {
    const result = await this.command._run({ keysAndAttributes })

    this.remainingRequestsCount--
    this.handleUnprocessedKeys(result)

    if (this.remainingRequestsCount === 0) {
      return result
    }

    if (this.command.tokenBucket) {
      const consumedCapacityUnits = result.rawResponse!.ConsumedCapacity!.reduce<number>(
        (capacityUnits, consumedCapacity) => {
          const { CapacityUnits } = consumedCapacity

          if (CapacityUnits) {
            return capacityUnits + CapacityUnits
          }

          return capacityUnits
        },
        0
      )

      await this.command.tokenBucket.removeTokens(consumedCapacityUnits)
    }

    return result
  }

  private handleUnprocessedKeys(result: CommandResult<Model[] | undefined, BatchGetItemCommandOutput>) {
    const unprocessedKeys =
      result.rawResponse.UnprocessedKeys && Object.values(result.rawResponse.UnprocessedKeys)[0]

    if (unprocessedKeys) {
      console.log(`Retry unprocessed keys: ${JSON.stringify(unprocessedKeys)}`)
      this.appendChunks(unprocessedKeys)
    }
  }

  async consume(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.on('readable', () => {
        while (this.read() !== null) {}
      })

      this.on('end', resolve)
    })
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<
    CommandResult<Model[] | undefined, BatchGetItemCommandOutput>
  > {
    return super[Symbol.asyncIterator]()
  }
}
