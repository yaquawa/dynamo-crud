import { PrimaryKeyCandidates, TokenBucket } from './types'
import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, BatchWriteItemCommandInput, WriteRequest } from '@aws-sdk/client-dynamodb'
import { BatchWriteItemCommandStream } from './stream'

export class BatchWriteItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly itemsPut?: Model[]
  private readonly itemsDelete?: PrimaryKeyCandidates<Model>[]
  public readonly tokenBucket?: TokenBucket
  private batchWriteItemCommandInput: Partial<BatchWriteItemCommandInput> = {}

  constructor(args: {
    client: DynamoDB
    tableName: string
    itemsPut?: Model[]
    itemsDelete?: PrimaryKeyCandidates<Model>[]
    tokenBucket?: TokenBucket
  }) {
    this.client = args.client
    this.tableName = args.tableName
    this.itemsPut = args.itemsPut
    this.itemsDelete = args.itemsDelete
    this.tokenBucket = args.tokenBucket
  }

  setCommandInput(options: Partial<BatchWriteItemCommandInput>): this {
    this.batchWriteItemCommandInput = { ...this.batchWriteItemCommandInput, ...options }
    return this
  }

  createReadableStream() {
    return new BatchWriteItemCommandStream<Model>({
      command: this,
    })
  }

  async run() {
    return this.createReadableStream().consume()
  }

  async _run({ writeRequests }: { writeRequests: WriteRequest[] }) {
    const rawResponse = await this.client.batchWriteItem({
      RequestItems: {
        [this.tableName]: writeRequests,
      },
      ...this.batchWriteItemCommandInput,
      ...(this.tokenBucket ? { ReturnConsumedCapacity: 'TOTAL' } : {}),
    })

    return new CommandResult(undefined, rawResponse)
  }

  getWriteRequests(): WriteRequest[] {
    if (this.itemsPut) {
      return this.itemsPut.map((item) => {
        return {
          PutRequest: { Item: marshall(item) },
        }
      })
    }

    if (this.itemsDelete) {
      return this.itemsDelete.map((primaryKey) => {
        return {
          DeleteRequest: { Key: marshall(primaryKey) },
        }
      })
    }

    throw new Error(`[dynamo-crud] Couldn't find 'itemsPut' or 'itemsDelete'`)
  }
}
