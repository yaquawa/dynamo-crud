import { GetItemCommand } from './GetItemCommand'
import { PutItemCommand } from './PutItemCommand'
import { DynamoDB } from '@aws-sdk/client-dynamodb'
import { UpdatableScanCommand } from './ScanCommand'
import { UpdatableQueryCommand } from './QueryCommand'
import { DeleteItemCommand } from './DeleteItemCommand'
import { UpdateItemCommand } from './UpdateItemCommand'
import { BatchGetItemCommand } from './BatchGetItemCommand'
import { BatchWriteItemCommand } from './BatchWriteItemCommand'
import { GetPrimaryKey, PrimaryKeyNameCandidates, TokenBucket } from './types'

export class DynamoTable<
  Model extends Record<string, any>,
  BasePK extends PrimaryKeyNameCandidates<Model>,
  BaseSK extends PrimaryKeyNameCandidates<Model> = never
> {
  private readonly client: DynamoDB
  public readonly tableName: string
  public readonly partitionKey: BasePK
  public readonly sortKey?: BaseSK
  private readonly tokenBucket?: TokenBucket

  constructor(args: {
    client: DynamoDB
    tableName: string
    partitionKey: BasePK
    sortKey?: BaseSK
    tokenBucket?: TokenBucket
  }) {
    this.client = args.client
    this.tableName = args.tableName
    this.partitionKey = args.partitionKey
    this.sortKey = args.sortKey
    this.tokenBucket = args.tokenBucket
  }

  scan() {
    const { client, tableName, tokenBucket, partitionKey: basePartitionKey, sortKey: baseSortKey } = this

    return new UpdatableScanCommand<Model>({
      client,
      tableName,
      basePartitionKey,
      baseSortKey,
      tokenBucket,
    })
  }

  update(primaryKey: GetPrimaryKey<Model, BasePK, BaseSK>) {
    const { client, tableName, tokenBucket } = this

    return new UpdateItemCommand<Model>({
      client,
      tableName,
      primaryKey: primaryKey as any,
      tokenBucket,
    })
  }

  query<IndexPrimaryKeyNames extends PrimaryKeyNameCandidates<Model>>(
    primaryKey: { [K in IndexPrimaryKeyNames]: Model[K] }
  ) {
    const { client, tableName, partitionKey: basePartitionKey, sortKey: baseSortKey, tokenBucket } = this
    return new UpdatableQueryCommand<Model, IndexPrimaryKeyNames, IndexPrimaryKeyNames>({
      client,
      tableName,
      primaryKey: primaryKey as any,
      basePartitionKey,
      baseSortKey,
      tokenBucket,
    })
  }

  find<PrimaryKey extends GetPrimaryKey<Model, BasePK, BaseSK> | GetPrimaryKey<Model, BasePK, BaseSK>[]>(
    primaryKey: PrimaryKey
  ): PrimaryKey extends GetPrimaryKey<Model, BasePK, BaseSK>[]
    ? BatchGetItemCommand<Model>
    : GetItemCommand<Model> {
    const { client, tableName, tokenBucket } = this

    if (Array.isArray(primaryKey)) {
      return new BatchGetItemCommand({ client, tableName, primaryKeys: primaryKey, tokenBucket }) as any
    }

    return new GetItemCommand({
      client,
      tableName,
      primaryKey: primaryKey as any,
      tokenBucket,
    }) as any
  }

  delete<PrimaryKey extends GetPrimaryKey<Model, BasePK, BaseSK> | GetPrimaryKey<Model, BasePK, BaseSK>[]>(
    primaryKey: PrimaryKey
  ): PrimaryKey extends GetPrimaryKey<Model, BasePK, BaseSK>[]
    ? BatchWriteItemCommand<Model>
    : DeleteItemCommand<Model> {
    const { client, tableName, tokenBucket } = this

    if (Array.isArray(primaryKey)) {
      return new BatchWriteItemCommand({ client, tableName, itemsDelete: primaryKey, tokenBucket }) as any
    }

    return new DeleteItemCommand({
      client,
      tableName,
      primaryKey: primaryKey as any,
      tokenBucket,
    }) as any
  }

  put<Item extends Model | Model[]>(
    item: Item
  ): Item extends Model[] ? BatchWriteItemCommand<Model> : PutItemCommand<Model> {
    const { client, tableName, tokenBucket } = this

    if (Array.isArray(item)) {
      return new BatchWriteItemCommand({ client, tableName, itemsPut: item, tokenBucket }) as any
    }

    return new PutItemCommand({
      client,
      tableName,
      item,
      tokenBucket,
    }) as any
  }
}
