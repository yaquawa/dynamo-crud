import { marshall } from '@aws-sdk/util-dynamodb'

const wait = (ms: number) => {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

export type PostModel = {
  id: number
  title: string
  url: string
  tags: string[]
  categories: Set<string>
  likeCount: number
  likes: number[]
  author: {
    id: number
    name: string
    followers: {
      name: string
    }[]
  }
  comments: {
    text: string
    likeCount: number
  }[]
}

export const post: PostModel = {
  id: 111,
  title: 'my post',
  url: 'https://mock-me.com',
  tags: ['tag-1', 'tag-2'],
  categories: new Set(['cat-1', 'cat-2']),
  likes: [2, 9, 1, 8],
  likeCount: 14,
  author: {
    id: 9,
    name: 'jest',
    followers: [
      {
        name: 'aha',
      },
    ],
  },
  comments: [
    {
      text: 'blabla',
      likeCount: 10,
    },
  ],
}

export class MockDynamoDB {
  async scan() {
    await wait(600)
    return {
      Items: [marshall(post), marshall(post), marshall(post)],
    }
  }

  async query() {
    await wait(300)

    return {
      Items: [marshall(post), marshall(post)],
    }
  }

  async getItem() {
    await wait(100)

    return {
      Item: marshall(post),
    }
  }

  async updateItem() {
    await wait(100)

    return undefined
  }

  async deleteItem() {
    await wait(100)

    return undefined
  }
}
