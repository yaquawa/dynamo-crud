import { CommandResult } from './CommandResult'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, GetItemCommandInput } from '@aws-sdk/client-dynamodb'
import { PrimaryKeyCandidates, TokenBucket } from './types'
import { selectAttributes } from './utils'

export class GetItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly primaryKey: PrimaryKeyCandidates<Model>
  private readonly tokenBucket?: TokenBucket
  private getItemCommandInput: Partial<GetItemCommandInput> = {}

  constructor(args: {
    client: DynamoDB
    tableName: string
    primaryKey: PrimaryKeyCandidates<Model>
    tokenBucket?: TokenBucket
  }) {
    this.client = args.client
    this.tableName = args.tableName
    this.primaryKey = args.primaryKey
    this.tokenBucket = args.tokenBucket
  }

  setCommandInput(getItemCommandInput: Partial<GetItemCommandInput>): this {
    this.getItemCommandInput = { ...this.getItemCommandInput, ...getItemCommandInput }
    return this
  }

  getCommandInput() {
    return this.getItemCommandInput
  }

  select(...attributeNames: (keyof Model)[]): this {
    selectAttributes(this.getCommandInput(), attributeNames as string[])

    return this
  }

  async run() {
    const rawResponse = await this.client.getItem({
      TableName: this.tableName,
      Key: marshall(this.primaryKey),
      ...this.getItemCommandInput,
      ...(this.tokenBucket ? { ReturnConsumedCapacity: 'TOTAL' } : {}),
    })

    const data = rawResponse.Item ? (unmarshall(rawResponse.Item) as Model) : undefined

    if (this.tokenBucket) {
      // TODO move to head
      const consumedCapacityUnits = rawResponse.ConsumedCapacity!.CapacityUnits!
      this.tokenBucket.removeTokens(consumedCapacityUnits)
    }

    return new CommandResult(data, rawResponse)
  }
}
