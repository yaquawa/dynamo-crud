import { CommandResult } from './CommandResult'
import { Updatable } from './UpdateItemCommand'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { PrimaryKeyNameCandidates } from './types'
import { DynamoDB, ScanCommandInput, ScanCommandOutput } from '@aws-sdk/client-dynamodb'

type ScanCommandRunOptions = { getAll?: boolean }

export class ScanCommand<Model extends Record<string, any>> {
  protected client: DynamoDB
  public readonly tableName: string
  private scanCommandInput: Partial<ScanCommandInput> = {}

  constructor({ client, tableName }: { client: DynamoDB; tableName: string }) {
    this.client = client
    this.tableName = tableName
  }

  setScanCommandInput(options: Partial<ScanCommandInput>) {
    this.scanCommandInput = { ...this.scanCommandInput, ...options }
    return this
  }

  index(indexName: string): this {
    return this.setScanCommandInput({
      IndexName: indexName,
    })
  }

  select(...attributeNames: (keyof Model)[]): this {
    return this.setScanCommandInput({
      ProjectionExpression: attributeNames.join(', '),
    })
  }

  limit(limit: number): this {
    return this.setScanCommandInput({ Limit: limit })
  }

  async run<Options extends ScanCommandRunOptions = { getAll: true }>(
    options?: Options
  ): Promise<
    CommandResult<
      Model[] | undefined,
      true extends Options['getAll'] ? ScanCommandOutput[] : ScanCommandOutput
    >
  > {
    const commandResult = await this.get()
    const defaultOptions: ScanCommandRunOptions = {
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

  private async get(options: Partial<ScanCommandInput> = {}) {
    const rawResponse = await this.client.scan({
      TableName: this.tableName,
      ...this.scanCommandInput,
      ...options,
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
