const kafka = require('../connection/index')
const { CompressionTypes } = require('kafkajs')

const consumer = kafka.consumer({ groupId: 'message-group' })
const producer = kafka.producer()

async function start () {
  await consumer.connect()
  await producer.connect()
  await consumer.subscribe({ topic: 'send-message' })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('RESPONSE CONSUMER TOPIC: ', topic)
      console.log('RESPONSE CONSUMER PARTITION: ', partition)
      console.log('RESPONSE CONSUMER MESSAGE: ', String(message.value))

      const payload = await JSON.parse(message.value)

      await producer.send({
        topic: 'response-message',
        compression: CompressionTypes.GZIP,
        messages: [
          { value: JSON.stringify(`Usuário ${payload.user.name} criado com sucesso!`) }
        ]
      })
    }
  })
}

start().catch(console.error)
