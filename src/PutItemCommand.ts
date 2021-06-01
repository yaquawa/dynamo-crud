import { CommandResult } from './CommandResult'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDB, PutItemCommandInput } from '@aws-sdk/client-dynamodb'
import { TokenBucket } from './types'

export class PutItemCommand<Model extends Record<string, any>> {
  private readonly client: DynamoDB
  private readonly tableName: string
  private readonly item: Model
  private readonly tokenBucket?: TokenBucket
  private putItemCommandInput: Partial<PutItemCommandInput> = {}

  constructor(args: { client: DynamoDB; tableName: string; item: Model; tokenBucket?: TokenBucket }) {
    this.client = args.client
    this.tableName = args.tableName
    this.item = args.item
    this.tokenBucket = args.tokenBucket
  }

  setCommandInput(putItemCommandInput: Partial<PutItemCommandInput>): this {
    this.putItemCommandInput = { ...this.putItemCommandInput, ...putItemCommandInput }
    return this
  }

  async run() {
    const rawResponse = await this.client.putItem({
      TableName: this.tableName,
      Item: marshall(this.item),
      ...this.putItemCommandInput,
      ...(this.tokenBucket ? { ReturnConsumedCapacity: 'TOTAL' } : {}),
    })

    if (this.tokenBucket) {
      // TODO move to head
      const consumedCapacityUnits = rawResponse.ConsumedCapacity!.CapacityUnits!
      this.tokenBucket.removeTokens(consumedCapacityUnits)
    }

    return new CommandResult(undefined, rawResponse)
  }
}
