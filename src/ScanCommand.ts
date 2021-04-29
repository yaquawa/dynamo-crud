import { CommandResult } from './CommandResult'
import { Updatable } from './UpdateItemCommand'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { PrimaryKeyNameCandidates } from './types'
import { DynamoDB, ScanCommandInput } from '@aws-sdk/client-dynamodb'
import { ReadItemsCommandReadableStream } from './ReadableStream'

export class ScanCommand<Model extends Record<string, any>> {
  protected client: DynamoDB
  public readonly tableName: string
  private scanCommandInput: Partial<ScanCommandInput> = {}

  constructor({ client, tableName }: { client: DynamoDB; tableName: string }) {
    this.client = client
    this.tableName = tableName
  }

  setCommandInput(options: Partial<ScanCommandInput>) {
    this.scanCommandInput = { ...this.scanCommandInput, ...options }
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

  createReadableStream(): ReadItemsCommandReadableStream<ScanCommand<Model>> {
    return new ReadItemsCommandReadableStream({ command: this })
  }

  async _run() {
    const rawResponse = await this.client.scan({
      TableName: this.tableName,
      ...this.scanCommandInput,
    })

    let items: Model[] | undefined

    if (rawResponse.Items && rawResponse.Items.length > 0) {
      items = rawResponse.Items.map((item) => unmarshall(item)) as Model[]
    }

    return new CommandResult(items, rawResponse)
  }
}

export class UpdatableScanCommand<Model extends Record<string, any>> extends ScanCommand<Model> {
  public readonly update: Updatable<Model, UpdatableScanCommand<Model>>

  constructor({
    client,
    tableName,
    basePartitionKey,
    baseSortKey,
  }: {
    client: DynamoDB
    tableName: string
    basePartitionKey: PrimaryKeyNameCandidates<Model>
    baseSortKey?: PrimaryKeyNameCandidates<Model>
  }) {
    super({ client, tableName })

    this.update = new Updatable<Model, UpdatableScanCommand<Model>>({
      client,
      tableName,
      basePartitionKey,
      baseSortKey,
      getItemsCommand: this,
    })
  }
}
