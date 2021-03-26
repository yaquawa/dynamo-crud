import { PostModel } from './data'
import { KeyConditionExpressionBuilder } from '../src'

describe('KeyConditionExpressionBuilder', () => {
  const builder = new KeyConditionExpressionBuilder<PostModel, 'id', 'title'>({ id: 100 })

  it('build without sort key condition', () => {
    expect(builder.compile()).toEqual({
      KeyConditionExpression: '#pk = :pv',
      ExpressionAttributeNames: { '#pk': 'id' },
      ExpressionAttributeValues: { ':pv': { N: '100' } },
    })
  })

  it('build with sort key condition', () => {
    expect(builder.where('title', '=', 'PostTitle').compile()).toEqual({
      KeyConditionExpression: '#pk = :pv AND #sk = :sv',
      ExpressionAttributeNames: { '#pk': 'id', '#sk': 'title' },
      ExpressionAttributeValues: { ':pv': { N: '100' }, ':sv': { S: 'PostTitle' } },
    })
  })
})
