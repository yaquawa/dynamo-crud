import { omit } from './utils'
import { ScanCommand } from './ScanCommand'
import { QueryCommand } from './QueryCommand'
import { Updatable } from './UpdateItemCommand'
import { Readable, ReadableOptions } from 'stream'
import { UnpackPromise } from './types'
import { CommandResult } from './CommandResult'
import { UpdateItemCommandOutput } from '@aws-sdk/client-dynamodb'

export class ReadItemsCommandReadableStream<
  Command extends QueryCommand<any, any, any> | ScanCommand<any>
> extends Readable {
  private command: Command

  constructor(options: ReadableOptions & { command: Command }) {
    super({
      ...omit(options, ['command']),
      ...{
        objectMode: true,
      },
    })

    this.command = options.command
  }

  _read() {
    this.command._run().then((result) => {
      const { data, rawResponse } = result

      if (data || rawResponse.LastEvaluatedKey) {
        if (data) {
          this.push(result)
        }

        if (rawResponse.LastEvaluatedKey) {
          this.command.setCommandInput({ ExclusiveStartKey: rawResponse.LastEvaluatedKey })
        } else {
          this.finishPush()
        }
      } else {
        this.finishPush()
      }
    })
  }

  private finishPush() {
    this.command.setCommandInput({ ExclusiveStartKey: undefined })
    this.push(null)
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<UnpackPromise<ReturnType<Command['_run']>>> {
    return super[Symbol.asyncIterator]()
  }
}

export class UpdateItemsCommandReadableStream<
  Model extends Record<string, any>,
  Command extends Updatable<Model, QueryCommand<Model, any, any> | ScanCommand<Model>>
> extends Readable {
  private getRunGenerator: AsyncGenerator<
    CommandResult<undefined, undefined> | CommandResult<undefined, UpdateItemCommandOutput[]>,
    void,
    unknown
  >

  constructor(options: ReadableOptions & { command: Command; shouldReturnValue?: boolean }) {
    super({
      ...omit(options, ['command', 'shouldReturnValue']),
      ...{
        objectMode: true,
      },
    })

    this.getRunGenerator = options.command.getRunGenerator({ shouldReturnValue: options.shouldReturnValue })
  }

  _read() {
    this.getRunGenerator.next().then((result) => {
      if (result.done) {
        this.push(null)
        return
      }

      this.push(result.value)
    })
  }

  async run(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.on('readable', () => {
        while (this.read() !== null) {}
      })

      this.on('end', resolve)
    })
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<
    CommandResult<undefined, undefined> | CommandResult<undefined, UpdateItemCommandOutput[]>
  > {
    return super[Symbol.asyncIterator]()
  }
}
