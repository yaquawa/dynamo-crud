import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, PutItemCommandInput } from '@aws-sdk/client-dynamodb'

export class PutItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly item: Model
  private putItemCommandInput: Partial<PutItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string; item: Model }) {
    this.client = args.client
    this.tableName = args.tableName
    this.item = args.item
  }

  setPutItemCommandInput(putItemCommandInput: Partial<PutItemCommandInput>): this {
    this.putItemCommandInput = { ...this.putItemCommandInput, ...putItemCommandInput }
    return this
  }

  async run() {
    const rawResponse = await this.client.putItem({
      TableName: this.tableName,
      Item: marshall(this.item),
      ...this.putItemCommandInput,
    })

    return new CommandResult(undefined, rawResponse)
  }
}
