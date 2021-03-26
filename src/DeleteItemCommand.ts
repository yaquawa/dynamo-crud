import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DeleteItemCommandInput, DynamoDB } from '@aws-sdk/client-dynamodb'
import { PrimaryKeyCandidates } from './types'

export class DeleteItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly primaryKey: PrimaryKeyCandidates<Model>
  private deleteItemCommandInput: Partial<DeleteItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string; primaryKey: PrimaryKeyCandidates<Model> }) {
    this.client = args.client
    this.tableName = args.tableName
    this.primaryKey = args.primaryKey
  }

  setDeleteItemCommandInput(deleteItemCommandInput: Partial<DeleteItemCommandInput>): this {
    this.deleteItemCommandInput = { ...this.deleteItemCommandInput, ...deleteItemCommandInput }
    return this
  }

  async run() {
    const rawResponse = await this.client.deleteItem({
      TableName: this.tableName,
      Key: marshall(this.primaryKey),
      ...this.deleteItemCommandInput,
    })

    return new CommandResult(undefined, rawResponse)
  }
}
