import { CommandResult } from './CommandResult'
import { Updatable } from './UpdateItemCommand'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { ReadItemsCommandReadableStream } from './ReadableStream'
import { GetPrimaryKey, PrimaryKeyNameCandidates } from './types'
import { DynamoDB, QueryCommandInput } from '@aws-sdk/client-dynamodb'
import { KeyConditionExpressionBuilder } from './KeyConditionExpressionBuilder'

export class QueryCommand<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never
> extends KeyConditionExpressionBuilder<Model, PK, SK> {
  protected client: DynamoDB
  public readonly tableName: string
  protected queryCommandInput: Partial<QueryCommandInput> = {}

  constructor({
    client,
    tableName,
    primaryKey,
  }: {
    client: DynamoDB
    tableName: string
    primaryKey: GetPrimaryKey<Model, PK, SK, false>
  }) {
    super(primaryKey)
    this.client = client
    this.tableName = tableName
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
  }: {
    client: DynamoDB
    tableName: string
    primaryKey: GetPrimaryKey<Model, PK, SK, false>
    basePartitionKey: PrimaryKeyNameCandidates<Model>
    baseSortKey?: PrimaryKeyNameCandidates<Model>
  }) {
    super({ primaryKey, client, tableName })

    this.update = new Updatable<Model, UpdatableQueryCommand<Model, PK, SK>>({
      client,
      tableName,
      basePartitionKey,
      baseSortKey,
      getItemsCommand: this,
    })
  }
}
