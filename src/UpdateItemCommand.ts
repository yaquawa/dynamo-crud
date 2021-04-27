import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { UpdateExpressionBuilder } from './UpdateExpressionBuilder'
import { DynamoDB, QueryCommandOutput } from '@aws-sdk/client-dynamodb'
import { PrimaryKeyCandidates, PrimaryKeyNameCandidates } from './types'
import { UpdateItemCommandInput } from '@aws-sdk/client-dynamodb/commands/UpdateItemCommand'

export class UpdateItemCommand<Model extends Record<string, any>> extends UpdateExpressionBuilder<Model> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly primaryKey: PrimaryKeyCandidates<Model>
  private updateItemCommandInput: Partial<UpdateItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string; primaryKey: PrimaryKeyCandidates<Model> }) {
    super()
    this.client = args.client
    this.tableName = args.tableName
    this.primaryKey = args.primaryKey
  }

  setUpdateItemCommandInput(updateItemCommandInput: Partial<UpdateItemCommandInput>): this {
    this.updateItemCommandInput = { ...this.updateItemCommandInput, ...updateItemCommandInput }
    return this
  }

  async run() {
    const { UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames } = this.compile()

    const rawResponse = await this.client.updateItem({
      TableName: this.tableName,
      Key: marshall(this.primaryKey),
      UpdateExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      ...this.updateItemCommandInput,
    })

    return new CommandResult(undefined, rawResponse)
  }
}

export class BatchUpdateItemCommand<
  Model extends Record<string, any>
> extends UpdateExpressionBuilder<Model> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private updateItemCommandInput: Partial<UpdateItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string }) {
    super()
    this.client = args.client
    this.tableName = args.tableName
  }

  setUpdateItemCommandInput(updateItemCommandInput: Partial<UpdateItemCommandInput>): this {
    this.updateItemCommandInput = { ...this.updateItemCommandInput, ...updateItemCommandInput }
    return this
  }

  async run(primaryKeys: PrimaryKeyCandidates<Model>[]) {
    const { UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames } = this.compile()

    const updatePromises = primaryKeys.map((primaryKey) => {
      return this.client.updateItem({
        TableName: this.tableName,
        Key: marshall(primaryKey),
        UpdateExpression,
        ExpressionAttributeValues,
        ExpressionAttributeNames,
        ...this.updateItemCommandInput,
      })
    })

    const rawResponses = await Promise.all(updatePromises)

    return new CommandResult(undefined, rawResponses)
  }
}

export class Updatable<
  Model extends Record<string, any>,
  GetItemsCommand extends { run(): Promise<CommandResult<Model[] | undefined, QueryCommandOutput[]>> }
> extends BatchUpdateItemCommand<Model> {
  private readonly getItemsCommand: GetItemsCommand
  private readonly basePartitionKey: PrimaryKeyNameCandidates<Model>
  private readonly baseSortKey?: PrimaryKeyNameCandidates<Model>

  constructor(args: {
    client: DynamoDB
    tableName: string
    basePartitionKey: PrimaryKeyNameCandidates<Model>
    baseSortKey?: PrimaryKeyNameCandidates<Model>
    getItemsCommand: GetItemsCommand
  }) {
    super(args)
    this.getItemsCommand = args.getItemsCommand
    this.basePartitionKey = args.basePartitionKey
    this.baseSortKey = args.baseSortKey
  }

  async run() {
    const shouldUpdate = this.compile().UpdateExpression !== ''

    if (!shouldUpdate) {
      return new CommandResult(undefined, [])
    }

    const items = (await this.getItemsCommand.run()).data
    if (!items) {
      return new CommandResult(undefined, [])
    }

    const { basePartitionKey, baseSortKey } = this
    const itemPrimaryKeys = items.map((item) => {
      return {
        [basePartitionKey]: item[basePartitionKey],
        ...(baseSortKey ? { [baseSortKey]: item[baseSortKey] } : {}),
      } as PrimaryKeyCandidates<Model>
    })

    return super.run(itemPrimaryKeys)
  }
}
