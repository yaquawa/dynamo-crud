import { ScanCommand } from './ScanCommand'
import { GetItemCommand } from './GetItemCommand'
import { PutItemCommand } from './PutItemCommand'
import { DynamoDB } from '@aws-sdk/client-dynamodb'
import { UpdatableQueryCommand } from './QueryCommand'
import { UpdateItemCommand } from './UpdateItemCommand'
import { DeleteItemCommand } from './DeleteItemCommand'
import { GetPrimaryKey, PrimaryKeyNameCandidates } from './types'

export class DynamoTable<
  Model extends Record<string, any>,
  BasePK extends PrimaryKeyNameCandidates<Model>,
  BaseSK extends PrimaryKeyNameCandidates<Model> = never
> {
  private readonly client: DynamoDB
  public readonly tableName: string

  constructor(args: { client: DynamoDB; tableName: string }) {
    this.client = args.client
    this.tableName = args.tableName
  }

  scan() {
    return new ScanCommand<Model>({
      client: this.client,
      tableName: this.tableName,
    })
  }

  update(primaryKey: GetPrimaryKey<Model, BasePK, BaseSK>) {
    return new UpdateItemCommand<Model>({
      client: this.client,
      tableName: this.tableName,
      primaryKey: primaryKey as any,
    })
  }

  query<IndexPrimaryKeyNames extends PrimaryKeyNameCandidates<Model>>(
    primaryKey: { [K in IndexPrimaryKeyNames]: Model[K] }
  ) {
    return new UpdatableQueryCommand<Model, IndexPrimaryKeyNames, IndexPrimaryKeyNames>({
      client: this.client,
      tableName: this.tableName,
      primaryKey: primaryKey as any,
    })
  }

  find(primaryKey: GetPrimaryKey<Model, BasePK, BaseSK>) {
    return new GetItemCommand({
      client: this.client,
      tableName: this.tableName,
      primaryKey: primaryKey as any,
    })
  }

  delete(primaryKey: GetPrimaryKey<Model, BasePK, BaseSK>) {
    return new DeleteItemCommand({
      client: this.client,
      tableName: this.tableName,
      primaryKey: primaryKey as any,
    })
  }

  put(item: Model) {
    return new PutItemCommand({
      client: this.client,
      tableName: this.tableName,
      item,
    })
  }
}
