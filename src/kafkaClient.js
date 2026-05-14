const { Kafka } = require('kafkajs');
require('dotenv').config();

// Load configuration from environment variables
const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const clientId = process.env.KAFKA_CLIENT_ID || 'antigravity-extension';

const kafka = new Kafka({
  clientId: clientId,
  brokers: brokers,
  // Add a resolver to handle internal K8s DNS names when running locally via port-forward
  brokerAddrResolver: async (broker) => {
    console.log(`Resolving broker: ${broker.host}:${broker.port}`);
    if (process.env.KAFKA_BROKERS && process.env.KAFKA_BROKERS.includes('localhost')) {
      console.log('Mapping to localhost:9092');
      return { host: 'localhost', port: 9092 };
    }
    return broker;
  },
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'antigravity-group' });

const run = async () => {
  // Producing
  await producer.connect();
  console.log('Producer connected');
  
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello Kafka from Antigravity!' },
    ],
  });
  console.log('Message sent successfully');

  // Consuming
  await consumer.connect();
  console.log('Consumer connected');
  
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);
