const kafka = require('../connection')
const { CompressionTypes } = require('kafkajs')

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'message-group-receiver' })

async function start () {
  await producer.connect()

  let iterator = 0

  while (iterator < 10) {
    const message = {
      user: {
        id: iterator + 1, name: `Usuário ${iterator + 1}`
      }
    }

    setTimeout(async () => {
      await producer.send({
        topic: 'send-message',
        compression: CompressionTypes.GZIP,
        messages: [
          { value: JSON.stringify(message) }
        ]
      })
    }, 1000)

    iterator++
  }

  // consumer do retorno
  await consumer.connect()

  await consumer.subscribe({ topic: 'response-message' })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('RESPONSE PRODUCER TOPIC: ', topic)
      console.log('RESPONSE PRODUCER PARTITION: ', partition)
      console.log('RESPONSE PRODUCER MESSAGE: ', String(message.value))
    }
  })
}

start().catch(console.error)
