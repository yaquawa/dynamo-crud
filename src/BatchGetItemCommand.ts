import { PrimaryKeyCandidates, TokenBucket } from './types'
import { CommandResult } from './CommandResult'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, KeysAndAttributes, BatchGetItemCommandInput } from '@aws-sdk/client-dynamodb'
import { BatchGetItemCommandStream } from './stream'

export class BatchGetItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private projectionExpression?: string
  private readonly primaryKeys: PrimaryKeyCandidates<Model>[]
  private batchGetItemCommandInput: Partial<BatchGetItemCommandInput> = {}
  public readonly tokenBucket?: TokenBucket

  constructor(args: {
    client: DynamoDB
    tableName: string
    primaryKeys: PrimaryKeyCandidates<Model>[]
    tokenBucket?: TokenBucket
  }) {
    this.client = args.client
    this.tableName = args.tableName
    this.primaryKeys = args.primaryKeys
    this.tokenBucket = args.tokenBucket
  }

  setCommandInput(options: Partial<BatchGetItemCommandInput>): this {
    this.batchGetItemCommandInput = { ...this.batchGetItemCommandInput, ...options }
    return this
  }

  select(...attributeNames: (keyof Model)[]): this {
    this.projectionExpression = attributeNames.join(', ')

    return this
  }

  createReadableStream() {
    return new BatchGetItemCommandStream<Model>({
      command: this,
    })
  }

  async run() {
    return this.createReadableStream().consume()
  }

  async _run({ keysAndAttributes }: { keysAndAttributes: KeysAndAttributes }) {
    const rawResponse = await this.client.batchGetItem({
      RequestItems: {
        [this.tableName]: keysAndAttributes,
      },
      ...this.batchGetItemCommandInput,
      ...(this.tokenBucket ? { ReturnConsumedCapacity: 'TOTAL' } : {}),
    })

    const items =
      rawResponse?.Responses && rawResponse.Responses[this.tableName]
        ? (unmarshall(rawResponse.Responses[this.tableName] as any) as Model[])
        : undefined

    return new CommandResult(items, rawResponse)
  }

  getKeysAndAttributes(): KeysAndAttributes {
    const Keys = this.primaryKeys.map((primaryKey) => {
      return marshall(primaryKey)
    })

    return {
      Keys,
      ...(this.projectionExpression ? { ProjectionExpression: this.projectionExpression } : {}),
    }
  }
}
