import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

interface SubscribeOpts {
  pollInterval: number
}

export class RedisStreams {
  constructor(private redis: Redis) {}

  async publish({
    streamName,
    message,
    maxLength = 10000,
  }: {
    streamName: string
    message: any
    maxLength?: number
  }) {
    await this.redis.xadd(
      streamName,
      'MAXLEN',
      '~',
      maxLength,
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
