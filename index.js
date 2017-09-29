const Redis = require('ioredis')
const amqp = require('amqplib')
const EventEmitter = require('events')
const moment = require('moment')
class Redis2Mq extends EventEmitter{
  constructor(opts){
    super()
    this.opts = opts
    this.redis = new Redis(opts.redis.port,opts.redis.host)
    this.initMq(opts.mq)
  }

  async initMq(config) {
    this.conn = await amqp.connect(`amqp://${config.username}:${config.passwd}@${config.host}:${config.port}`)
    this.conn.on('error', (err) => console.log('err', err))
    this.conn.on('close', () => console.log('close'))
    this.conn.on('blocked', (reason) => console.log('reason', reason))
    this.conn.on('unblocked', () => console.log('unblocked'))
    this.ch = await this.conn.createChannel()
    await this.ch.prefetch(1)
    this.emit('inited')
  }

  start() {
    this.on('inited',async () => {
      const res = await this.redis.psubscribe('__key*__:*')
      if(res > 0) {
        console.log(`[${moment().format('YYYY-MM-DD HH:mm:ss.SSS')}] 订阅了批量通道：__key*__:*`)
      }
      this.redis.on('pmessage', async(pattern, channel, message) => {
        console.log(`[${moment().format('YYYY-MM-DD HH:mm:ss.SSS')}] 收到超时通知，内容为${message}`)
        const arr =  message.split(':')
        arr.pop()
        await this.ch.assertExchange(this.opts.exchange, 'topic', {
          durable: true
        })
        const key = arr.join('.')
        await this.ch.publish(this.opts.exchange, key, Buffer.from(message), {
          persistent: true
        })
      })
    })
  }
}

module.exports = Redis2Mq