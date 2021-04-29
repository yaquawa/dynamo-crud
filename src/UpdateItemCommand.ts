import { runParallel } from './utils'
import { ScanCommand } from './ScanCommand'
import { QueryCommand } from './QueryCommand'
import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { UpdateExpressionBuilder } from './UpdateExpressionBuilder'
import { UpdateItemsCommandReadableStream } from './ReadableStream'
import { PrimaryKeyCandidates, PrimaryKeyNameCandidates } from './types'
import { DynamoDB, UpdateItemCommandOutput } from '@aws-sdk/client-dynamodb'
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

  setCommandInput(updateItemCommandInput: Partial<UpdateItemCommandInput>): this {
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

  setCommandInput(updateItemCommandInput: Partial<UpdateItemCommandInput>): this {
    this.updateItemCommandInput = { ...this.updateItemCommandInput, ...updateItemCommandInput }
    return this
  }

  async run(primaryKeys: PrimaryKeyCandidates<Model>[]): Promise<void> {
    return this._run({ primaryKeys })
  }

  async _run<ShouldReturnValue extends boolean = false>({
    primaryKeys,
    shouldReturnValue,
  }: {
    primaryKeys: PrimaryKeyCandidates<Model>[]
    shouldReturnValue?: ShouldReturnValue
  }): Promise<true extends ShouldReturnValue ? CommandResult<undefined, UpdateItemCommandOutput[]> : void> {
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

    if (!shouldReturnValue) {
      return runParallel(updatePromises) as any
    }

    const rawResponses = await Promise.all(updatePromises)

    return new CommandResult(undefined, rawResponses) as any
  }
}

export class Updatable<
  Model extends Record<string, any>,
  GetItemsCommand extends QueryCommand<Model, any, any> | ScanCommand<Model>
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

  createReadableStream(): UpdateItemsCommandReadableStream<Model, Updatable<Model, GetItemsCommand>> {
    return new UpdateItemsCommandReadableStream({ command: this, shouldReturnValue: true })
  }

  async run(): Promise<void> {
    return new UpdateItemsCommandReadableStream<Model, Updatable<Model, GetItemsCommand>>({
      command: this,
      shouldReturnValue: false,
    }).run()
  }

  async *getRunGenerator({ shouldReturnValue = false }: { shouldReturnValue?: boolean } = {}) {
    const shouldUpdate = this.compile().UpdateExpression !== ''

    if (!shouldUpdate) {
      yield new CommandResult(undefined, undefined)

      return
    }

    for await (const result of this.getItemsCommand.createReadableStream()) {
      const items = result.data as Model[]

      const { basePartitionKey, baseSortKey } = this
      const itemPrimaryKeys = items.map((item) => {
        return {
          [basePartitionKey]: item[basePartitionKey],
          ...(baseSortKey ? { [baseSortKey]: item[baseSortKey] } : {}),
        } as PrimaryKeyCandidates<Model>
      })

      yield super._run({ primaryKeys: itemPrimaryKeys, shouldReturnValue })
    }
  }
}
