import { PostModel } from './data'
import { UpdateExpressionBuilder } from '../src'

describe('KeyConditionExpressionBuilder', () => {
  const builder = new UpdateExpressionBuilder<PostModel>()

  it('build update expression', () => {
    expect(
      builder
        .set('title', 'PostTitle')
        .set('tags', ['tag1', 'tag2'])
        .set('author.name', 'foobar')
        .setCalc('likeCount', 'id', '+', 'author.id')
        .setIfNotExists('author.name', 'foobar')
        .setListAppend('tags', ['tag-1', 'tag-2'])
        .delete('categories', new Set(['cat-1']))
        .remove('author.followers.0.name')
        .add('likeCount', 1)
        .add('comments.2.likeCount', 5)
        .compile()
    ).toEqual({
      ExpressionAttributeNames: {
        '#p0': 'title',
        '#p1': 'tags',
        '#p10': 'comments.2.likeCount',
        '#p11': 'categories',
        '#p2': 'author.name',
        '#p3': 'likeCount',
        '#p4': 'id',
        '#p5': 'author.id',
        '#p6': 'author.name',
        '#p7': 'tags',
        '#p8': 'author.followers.0.name',
        '#p9': 'likeCount',
      },
      ExpressionAttributeValues: {
        ':v0': { S: 'PostTitle' },
        ':v1': { L: [{ S: 'tag1' }, { S: 'tag2' }] },
        ':v2': { S: 'foobar' },
        ':v3': { S: 'foobar' },
        ':v4': { L: [{ S: 'tag-1' }, { S: 'tag-2' }] },
        ':v5': { N: '1' },
        ':v6': { N: '5' },
        ':v7': { SS: ['cat-1'] },
      },
      UpdateExpression:
        'SET #p0 = :v0, #p1 = :v1, #p2 = :v2, #p3 = #p4 + #p5, #p6 = if_not_exists(#p6, :v3), #p7 = list_append(#p7, :v4)\nREMOVE #p8\nADD #p9 :v5, #p10 :v6\nDELETE #p11 :v7',
    })
  })
})
