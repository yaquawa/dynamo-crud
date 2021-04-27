import { MockDynamoDB, post, PostModel } from './data'
import { DynamoTable } from '../src'
import { DynamoDB } from '@aws-sdk/client-dynamodb'

const mockDynamoDB = (new MockDynamoDB() as unknown) as DynamoDB

describe('DynamoTable', () => {
  const dynamoTable = new DynamoTable<PostModel, 'id', 'title'>({
    client: mockDynamoDB,
    tableName: 'posts',
    partitionKey: 'id',
    sortKey: 'title',
  })

  test('query items', async () => {
    const { data } = await dynamoTable
      .query({ id: 100, title: 'hello' })
      .index('byTitle')
      .where('title', '=', 'myTitle')
      .run()

    expect(data).toEqual([post])
  })

  test('scan items', async () => {
    const { data } = await dynamoTable.scan().run()

    expect(data).toEqual([post])
  })

  test('query then update items', async () => {
    const { data } = await dynamoTable
      .query({ id: 100, title: 'hello' })
      .index('byTitle')
      .where('title', '=', 'myTitle')
      .update.set('author.id', 100)
      .run()

    expect(data).toEqual(undefined)
  })

  test('get item', async () => {
    const { data } = await dynamoTable.find({ id: 100, title: 'hello' }).run()

    expect(data).toEqual(post)
  })

  test('update item', async () => {
    const { data } = await dynamoTable.update({ id: 100, title: 'hello' }).set('author.name', 'foo').run()

    expect(data).toEqual(undefined)
  })

  test('delete item', async () => {
    const { data } = await dynamoTable.delete({ id: 100, title: 'hello' }).run()

    expect(data).toEqual(undefined)
  })
})
