const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'kafka',
  brokers: ['localhost:9092'],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
})

module.exports = kafka
