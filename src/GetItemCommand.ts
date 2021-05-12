import { CommandResult } from './CommandResult'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, GetItemCommandInput } from '@aws-sdk/client-dynamodb'
import { PrimaryKeyCandidates } from './types'

export class GetItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly primaryKey: PrimaryKeyCandidates<Model>
  private getItemCommandInput: Partial<GetItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string; primaryKey: PrimaryKeyCandidates<Model> }) {
    this.client = args.client
    this.tableName = args.tableName
    this.primaryKey = args.primaryKey
  }

  setCommandInput(getItemCommandInput: Partial<GetItemCommandInput>): this {
    this.getItemCommandInput = { ...this.getItemCommandInput, ...getItemCommandInput }
    return this
  }

  select(...attributeNames: (keyof Model)[]): this {
    return this.setCommandInput({
      ProjectionExpression: attributeNames.join(', '),
    })
  }

  async run() {
    const rawResponse = await this.client.getItem({
      TableName: this.tableName,
      Key: marshall(this.primaryKey),
      ...this.getItemCommandInput,
    })
    const data = rawResponse.Item ? (unmarshall(rawResponse.Item) as Model) : undefined

    return new CommandResult(data, rawResponse)
  }
}
