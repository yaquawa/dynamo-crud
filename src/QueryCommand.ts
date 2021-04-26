import { DynamoDB, QueryCommandOutput } from '@aws-sdk/client-dynamodb'
import { QueryCommandInput } from '@aws-sdk/client-dynamodb/commands/QueryCommand'
import { KeyConditionExpressionBuilder } from './KeyConditionExpressionBuilder'
import { BatchUpdateItemCommand } from './UpdateItemCommand'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { CommandResult } from './CommandResult'
import { GetPrimaryKey, PrimaryKeyCandidates, PrimaryKeyNameCandidates } from './types'

type QueryCommandRunOptions = { getAll?: boolean }

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

  setQueryCommandInput(options: Partial<QueryCommandInput>): this {
    this.queryCommandInput = { ...this.queryCommandInput, ...options }

    return this
  }

  index(indexName: string): this {
    return this.setQueryCommandInput({
      IndexName: indexName,
    })
  }

  select(...attributeNames: (keyof Model)[]): this {
    return this.setQueryCommandInput({
      ProjectionExpression: attributeNames.join(', '),
    })
  }

  limit(limit: number): this {
    return this.setQueryCommandInput({ Limit: limit })
  }

  async run<Options extends QueryCommandRunOptions = { getAll: true }>(
    options?: Options
  ): Promise<
    CommandResult<
      Model[] | undefined,
      true extends Options['getAll'] ? QueryCommandOutput[] : QueryCommandOutput
    >
  > {
    const commandResult = await this.get()
    const defaultOptions: QueryCommandRunOptions = {
      getAll: true,
    }

    options = { ...defaultOptions, ...(options || {}) } as Options

    if (options.getAll) {
      let items = commandResult.data
      let rawResponses = [commandResult.rawResponse]
      let LastEvaluatedKey = commandResult.rawResponse.LastEvaluatedKey

      while (LastEvaluatedKey) {
        const result = await this.get({ ExclusiveStartKey: LastEvaluatedKey })

        if (result.data) {
          items = items?.concat(result.data)
        }

        rawResponses.push(result.rawResponse)

        LastEvaluatedKey = result.rawResponse.LastEvaluatedKey
      }

      return new CommandResult(items, rawResponses) as any
    }

    return commandResult as any
  }

  private async get(options: Partial<QueryCommandInput> = {}) {
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
      ...options,
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
  public readonly update: Updatable<Model, PK, SK>

  constructor({
    client,
    tableName,
    primaryKey,
  }: {
    client: DynamoDB
    tableName: string
    primaryKey: GetPrimaryKey<Model, PK, SK, false>
  }) {
    super({ primaryKey, client, tableName })

    this.update = new Updatable<Model, PK, SK>({ client, tableName, queryCommand: this })
  }
}

class Updatable<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never
> extends BatchUpdateItemCommand<Model> {
  private queryCommand: UpdatableQueryCommand<Model, PK, SK>

  constructor(args: {
    client: DynamoDB
    tableName: string
    queryCommand: UpdatableQueryCommand<Model, PK, SK>
  }) {
    super(args)
    this.queryCommand = args.queryCommand
  }

  async run() {
    const items = (await this.queryCommand.run()).data
    const shouldUpdate = this.compile().UpdateExpression !== ''
    const sortKeyName = this.queryCommand.getSortKeyName()
    const partitionKeyName = this.queryCommand.partitionKeyName

    if (items && items.length && shouldUpdate) {
      const itemPrimaryKeys = items.map((item) => {
        return {
          [partitionKeyName]: item[partitionKeyName],
          ...(sortKeyName ? { [sortKeyName]: item[sortKeyName] } : {}),
        } as PrimaryKeyCandidates<Model>
      })

      return super.run(itemPrimaryKeys)
    }

    return new CommandResult(undefined, [])
  }
}
