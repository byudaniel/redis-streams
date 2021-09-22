import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

interface SubscribeOpts {
  pollInterval?: number
  subscribeFromStart: boolean
}

export class RedisStreams {
  constructor(private redis: Redis) {}

  async publish(
    message: any,
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
    consumerGroupName: string,
    messageId: string
  ) {
    await this.redis.xack(streamName, consumerGroupName, messageId)
  }

  subscribe(
    streamName: string,
    groupName: string,
    handler: ({
      message,
      ack,
    }: {
      message: any
      ack: () => Promise<void>
    }) => any,
    opts?: SubscribeOpts
  ) {
    setTimeout(async () => {
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

      const consumerGroupName = nanoid()
      const data = await this.redis.xreadgroup(
        'GROUP',
        groupName,
        consumerGroupName,
        'BLOCK',
        opts?.pollInterval || 60000,
        'COUNT',
        1,
        'STREAMS',
        streamName,
        '>'
      )

      if (data) {
        for (const streams of data) {
          for (const inner of streams[1]) {
            handler({
              message: JSON.parse(inner[1][1]),
              ack: () => {
                return this.ackMessage(streamName, consumerGroupName, inner[0])
              },
            })
          }
        }
      }

      this.subscribe(streamName, groupName, handler, opts)
    }, 0)
  }
}
