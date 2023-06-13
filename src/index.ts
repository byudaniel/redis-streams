import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

interface SubscribeOpts {
  pollInterval?: number
  subscribeFromStart: boolean
  consumerName?: string
  disableCreateGroup?: boolean
}

export class RedisStreams {
  constructor(private redis: Redis) {}

  async publish<T>(
    message: T,
    streamName: string,
    opts: {
      maxLength?: number
    } = {}
  ): Promise<void> {
    await this.redis.xadd(
      streamName,
      'MAXLEN',
      '~',
      opts.maxLength || 1000000,
      '*',
      'json',
      JSON.stringify(message)
    )
  }

  private async ackMessage(
    streamName: string,
    consumerName: string,
    messageId: string
  ) {
    await this.redis.xack(streamName, consumerName, messageId)
  }

  subscribe<T>(
    streamName: string,
    groupName: string,
    handler: ({
      message,
      ack,
    }: {
      message: T
      ack: () => Promise<void>
    }) => void,
    opts?: SubscribeOpts
  ) {
    setTimeout(async () => {
      if (!opts?.disableCreateGroup) {
        try {
          await this.redis.xgroup(
            'CREATE',
            streamName,
            groupName,
            opts?.subscribeFromStart ? 0 : '$',
            'MKSTREAM'
          )
        } catch (err) {
          if (
            err instanceof Error &&
            !err.message.toLowerCase().includes('already exists')
          ) {
            throw err
          }
        }
      }

      const consumerName = opts?.consumerName || nanoid()
      const data = (await this.redis.xreadgroup(
        'GROUP',
        groupName,
        consumerName, // @ts-ignore - appears to be error in types, will have to investigate
        'BLOCK',
        opts?.pollInterval || 60000,
        'COUNT',
        1,
        'STREAMS',
        streamName,
        '>'
      )) as any[] // TODO: Better typing

      if (data) {
        for (const streams of data) {
          for (const inner of streams[1]) {
            handler({
              message: JSON.parse(inner[1][1]),
              ack: () => {
                return this.ackMessage(streamName, groupName, inner[0])
              },
            })
          }
        }
      }

      this.subscribe(streamName, groupName, handler, {
        subscribeFromStart: false,
        ...opts,
        consumerName,
        disableCreateGroup: true,
      })
    }, 0)
  }
}
