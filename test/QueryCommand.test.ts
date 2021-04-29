import { DynamoDB } from '@aws-sdk/client-dynamodb'
import { MockDynamoDB, post, PostModel } from './data'
import { QueryCommand, UpdatableQueryCommand } from '../src'

const mockDynamoDB = (new MockDynamoDB() as unknown) as DynamoDB

describe('QueryCommand', () => {
  let queryCommand: QueryCommand<PostModel, 'id', 'title'>

  beforeEach(() => {
    queryCommand = new QueryCommand<PostModel, 'id', 'title'>({
      client: mockDynamoDB,
      tableName: 'posts',
      primaryKey: { id: 100, title: 'myTitle' },
    })
  })

  it('return item', async () => {
    const readableStream = queryCommand.index('byTitle').where('title', '=', 'my post').createReadableStream()

    let items: any[] = []

    for await (const { data } of readableStream) {
      items = items.concat(data)
    }

    expect(items).toEqual([post, post])
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
    basePartitionKey: 'id',
    baseSortKey: 'title',
  })

  it('returns items', async () => {
    const readableStream = queryCommand.index('byTitle').where('title', '=', 'my post').createReadableStream()

    let items: any[] = []

    for await (const { data } of readableStream) {
      items = items.concat(data)
    }

    expect(items).toEqual([post, post])
  })

  it('can update items after query', async () => {
    await queryCommand.update
      .set('author.id', 10)
      .delete('categories', new Set(['cat-1']))
      .run()
  })
})
