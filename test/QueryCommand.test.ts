import { MockDynamoDB, post, PostModel } from './data'
import { QueryCommand, UpdatableQueryCommand } from '../src'
import { DynamoDB } from '@aws-sdk/client-dynamodb'

const mockDynamoDB = (new MockDynamoDB() as unknown) as DynamoDB

describe('QueryCommand', () => {
  const queryCommand = new QueryCommand<PostModel, 'id', 'title'>({
    client: mockDynamoDB,
    tableName: 'posts',
    primaryKey: { id: 100, title: 'myTitle' },
  })

  it('return item', async () => {
    const result = await queryCommand.index('byTitle').where('title', '=', 'my post').run()

    expect(result.data).toEqual([post])
  })
})

describe('UpdatableQueryCommand', () => {
  const queryCommand = new UpdatableQueryCommand<PostModel, 'id', 'title'>({
    client: mockDynamoDB,
    tableName: 'posts',
    primaryKey: {
      id: 100,
      title: 'myTitle',
    },
  })

  it('returns items', async () => {
    const result = await queryCommand.index('byTitle').where('title', '=', 'my post').run()

    expect(result.data).toEqual([post])
  })

  it('can update items after query', async () => {
    const result = await queryCommand.update
      .set('author.id', 10)
      .delete('categories', new Set(['cat-1']))
      .run()

    expect(result.data).toEqual(undefined)
  })
})
