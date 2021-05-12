import { CommandResult } from './CommandResult'
import { Updatable } from './UpdateItemCommand'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { ReadItemsCommandReadableStream } from './stream'
import { GetPrimaryKey, PrimaryKeyNameCandidates, TokenBucket } from './types'
import { DynamoDB, QueryCommandInput } from '@aws-sdk/client-dynamodb'
import { KeyConditionExpressionBuilder } from './KeyConditionExpressionBuilder'

export class QueryCommand<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never
> extends KeyConditionExpressionBuilder<Model, PK, SK> {
  protected client: DynamoDB
  public readonly tableName: string
  public readonly tokenBucket?: TokenBucket
  protected queryCommandInput: Partial<QueryCommandInput> = {}

  constructor({
    client,
    tableName,
    primaryKey,
    tokenBucket,
  }: {
    client: DynamoDB
    tableName: string
    primaryKey: GetPrimaryKey<Model, PK, SK, false>
    tokenBucket?: TokenBucket
  }) {
    super(primaryKey)
    this.client = client
    this.tableName = tableName
    this.tokenBucket = tokenBucket
  }

  setCommandInput(options: Partial<QueryCommandInput>): this {
    this.queryCommandInput = { ...this.queryCommandInput, ...options }

    return this
  }

  index(indexName: string): this {
    return this.setCommandInput({
      IndexName: indexName,
    })
  }

  select(...attributeNames: (keyof Model)[]): this {
    return this.setCommandInput({
      ProjectionExpression: attributeNames.join(', '),
    })
  }

  limit(limit: number): this {
    return this.setCommandInput({ Limit: limit })
  }

  createReadableStream(): ReadItemsCommandReadableStream<QueryCommand<Model, PK, SK>> {
    return new ReadItemsCommandReadableStream({ command: this })
  }

  async _run() {
    const {
      KeyConditionExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
    } = this.compile() as any

    const rawResponse = await this.client.query({
      TableName: this.tableName,
      KeyConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ...this.queryCommandInput,
      ...(this.tokenBucket ? { ReturnConsumedCapacity: 'TOTAL' } : {}),
    })

    let items: Model[] | undefined

    if (rawResponse.Items && rawResponse.Items.length > 0) {
      items = rawResponse.Items.map((item) => unmarshall(item)) as Model[]
    }

    return new CommandResult(items, rawResponse)
  }
}

export class UpdatableQueryCommand<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never
> extends QueryCommand<Model, PK, SK> {
  public readonly update: Updatable<Model, UpdatableQueryCommand<Model, PK, SK>>

  constructor({
    client,
    tableName,
    primaryKey,
    basePartitionKey,
    baseSortKey,
    tokenBucket,
  }: {
    client: DynamoDB
    tableName: string
    primaryKey: GetPrimaryKey<Model, PK, SK, false>
    basePartitionKey: PrimaryKeyNameCandidates<Model>
    baseSortKey?: PrimaryKeyNameCandidates<Model>
    tokenBucket?: TokenBucket
  }) {
    super({ primaryKey, client, tableName })

    this.update = new Updatable<Model, UpdatableQueryCommand<Model, PK, SK>>({
      client,
      tableName,
      basePartitionKey,
      baseSortKey,
      getItemsCommand: this,
      tokenBucket,
    })
  }
}
