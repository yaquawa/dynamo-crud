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
    ).toMatchSnapshot()
  })
})
