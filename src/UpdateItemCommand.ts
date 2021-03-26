import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB } from '@aws-sdk/client-dynamodb'
import { UpdateExpressionBuilder } from './UpdateExpressionBuilder'
import { UpdateItemCommandInput } from '@aws-sdk/client-dynamodb/commands/UpdateItemCommand'
import { CommandResult } from './CommandResult'
import { PrimaryKeyCandidates } from './types'

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
