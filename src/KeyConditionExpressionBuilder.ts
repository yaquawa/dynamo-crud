import { marshall } from '@aws-sdk/util-dynamodb'
import { GetPrimaryKey, PrimaryKeyNameCandidates } from './types'

type ComparisonOperator = '=' | '<' | '<=' | '>' | '>=' | 'BETWEEN' | 'begins_with'

export class KeyConditionExpressionBuilder<
  Model extends Record<string, any>,
  PK extends PrimaryKeyNameCandidates<Model>,
  SK extends PrimaryKeyNameCandidates<Model> = never
> {
  public readonly partitionKeyName: PK
  public readonly partitionKeyValue: Model[PK]

  private sortKeyCondition: string | undefined
  private sortKeyName: SK | undefined
  private sortKeyValue1: Model[SK] | undefined
  private sortKeyValue2: Model[SK] | undefined

  /**
   * @param primaryKey [Note] PK should be the first key of the object.
   */
  constructor(primaryKey: GetPrimaryKey<Model, PK, SK, false>) {
    const [pk, sk] = Object.entries(primaryKey)

    this.partitionKeyName = pk[0] as PK
    this.partitionKeyValue = pk[1]

    if (sk) {
      this.where(sk[0] as SK, '=', sk[1])
    }
  }

  where(
    sortKeyName: SK,
    comparisonOperator: ComparisonOperator,
    ...sortKeyValues: ComparisonOperator extends 'BETWEEN' ? [Model[SK], Model[SK]] : [Model[SK]]
  ): this {
    this.sortKeyName = sortKeyName
    this.sortKeyValue1 = sortKeyValues[0]
    this.sortKeyValue2 = '1' in sortKeyValues ? sortKeyValues[1] : undefined

    if (comparisonOperator === 'BETWEEN') {
      this.sortKeyCondition = `#sk BETWEEN :sv AND :sv2`

      return this
    }

    if (comparisonOperator === 'begins_with') {
      this.sortKeyCondition = `begins_with( #sk, :sv )`

      return this
    }

    this.sortKeyCondition = `#sk ${comparisonOperator} :sv`

    return this
  }

  getSortKeyName() {
    return this.sortKeyName
  }

  compile() {
    const KeyConditionExpression =
      '#pk = :pv' + (this.sortKeyCondition ? ` AND ${this.sortKeyCondition}` : '')

    const ExpressionAttributeValues: { ':pv': any; ':sv'?: any; ':sv2'?: any } = {
      ':pv': this.partitionKeyValue,
    }
    if (this.sortKeyValue1) {
      ExpressionAttributeValues[':sv'] = this.sortKeyValue1
    }
    if (this.sortKeyValue2) {
      ExpressionAttributeValues[':sv2'] = this.sortKeyValue2
    }

    const ExpressionAttributeNames: {
      '#pk': PK
      '#sk'?: SK
    } = {
      '#pk': this.partitionKeyName,
    }
    if (this.sortKeyName) {
      ExpressionAttributeNames['#sk'] = this.sortKeyName
    }

    return {
      KeyConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues),
    }
  }
}
