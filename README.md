# Dynamo-CRUD

A wrapper lib for the official SDK which provide an easier to use CRUD API and type safety.

## Main Features
* Easier to understand fluid APIs
* Auto marshall/unmarshall the DTO object
* Use Stream API for scan and query command
* Transparently use Batch GetItem/WriteItem
* Advanced type hinting for TypeScript users
* Adjust requests size automatically on BatchWriteItem

## Installation
```bash
npm i dynamo-crud
```

## Basic Usage

```js
import { DynamoTable } from "dynamo-crud"
import { DynamoDB } from "@aws-sdk/client-dynamodb"

const dynamoTable = new DynamoTable({
  client: new DynamoDB({ region: process.env.REGION }),
  tableName: "posts",
  partitionKey: "id",
  sortKey: "title",
})

async function run() {
  /*
  |---------------------------------------------------------------------------
  | scan items
  |---------------------------------------------------------------------------
  */
  const readableStreamScan = dynamoTable.scan().createReadableStream()

  for await (const { data, rawResponse } of readableStreamScan) {
    console.log(data, rawResponse)
  }

  /*
  |---------------------------------------------------------------------------
  | query items
  |---------------------------------------------------------------------------
  */
  const readableStreamQuery = dynamoTable
  .query({ id: 100 })
  .index("byTitle")
  .where("title", "=", "myTitle")
  .createReadableStream()

  for await (const { data, rawResponse } of readableStreamQuery) {
    console.log(data, rawResponse)
  }

  /*
  |---------------------------------------------------------------------------
  | Get a single item (GetItem)
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .find({ id: 100, title: "hello" })
  .run()

  /*
  |---------------------------------------------------------------------------
  | Get mutiple items (BatchGetItem)
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .find([ { id: 100, title: "hello" }, { id: 99, title: "hi" } ])
  .run()

  /*
  |---------------------------------------------------------------------------
  | Delete a single item
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .delete({ id: 100, title: "hello" })
  .run()

  /*
  |---------------------------------------------------------------------------
  | Delete mutiple items (BatchWriteItem)
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .delete([ { id: 100, title: "hello" }, { id: 99, title: "hi" } ])
  .run()

  /*
  |---------------------------------------------------------------------------
  | Put a single item (PutItem)
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .put({ id: 100, title: "hello" })
  .run()

  /*
  |---------------------------------------------------------------------------
  | Put mutiple items (BatchWriteItem)
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .put([ { id: 100, title: "hello" }, { id: 99, title: "hi" } ])
  .run()

  /*
  |---------------------------------------------------------------------------
  | update a single item
  |---------------------------------------------------------------------------
  */
  let { data, rawResponse } = await dynamoTable
  .update({ id: 100, title: "hello" })
  .set("title", "hello world")
  .add("likeCount", 1)
  .delete("categories", new Set(["cat-1"]))
  .run()

  
  /*
  |---------------------------------------------------------------------------
  | query then update the quired items
  |---------------------------------------------------------------------------
  */
  await dynamoTable
  .query({ id: 100 })
  .index("byLikeCount")
  .where("likeCount", ">", 100)
  .update.set("author.id", 100)
  .run() // or call `.createReadableStream()` if you are going to use the returned raw response.
}

run()
```

## TypeScript
To obtain the type hinting, give the type of the table structure, the partition key, the sort key.

For example:

```ts
export type PostModel = {
  id: number
  title: string
  tags: string[]
  categories: Set<string>
  likeCount: number
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

// PK: 'id'  SK: 'title'
const dynamoTable = new DynamoTable<PostModel, 'id', 'title'>({
  client: mockDynamoDB,
  tableName: 'posts',
  partitionKey: "id",
  sortKey: "title",
})


dynamoTable.query({id: '100'})
// error: `id` should be number


dynamoTable
  .update({id: 100}) // [error] `title` is required
  .set('author.name_x', 'foo') // [error] `author.name_x` is not the correct path
  .set('comments.0.likeCount', '1000') // [error] `likeCount` should be `number`
  .add('tags', 1000) // [error] `tags` is not allowed to perferm `add` operation
  .delete('tags', 'tag-1') // [error] `tags` is not allowed to perferm `delete` operation
```
