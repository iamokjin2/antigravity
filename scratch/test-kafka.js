const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'test-client',
  brokers: ['localhost:31175'],
  connectionTimeout: 5000,
  requestTimeout: 25000,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group-' + Date.now() });

const run = async () => {
  const topic = 'test-topic';
  console.log('🚀 Starting Kafka test...');

  try {
    // 1. Producer 연결 및 메시지 전송
    await producer.connect();
    console.log('✅ Producer connected');

    const testMessage = `Hello Kafka! Test at ${new Date().toISOString()}`;
    await producer.send({
      topic,
      messages: [{ value: testMessage }],
    });
    console.log(`📤 Sent message: ${testMessage}`);

    // 2. Consumer 연결 및 구독
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log('✅ Consumer connected and subscribed');

    let messageReceived = false;
    
    // 타임아웃 설정 (10초 동안 메시지 기다림)
    const timeout = setTimeout(async () => {
      if (!messageReceived) {
        console.log('❌ Kafka communication test FAILED (Timeout - No message received)');
        await cleanup();
      }
    }, 10000);

    const cleanup = async () => {
      clearTimeout(timeout);
      await producer.disconnect();
      await consumer.disconnect();
      console.log('🏁 Test finished and disconnected');
      process.exit(0);
    };

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`📩 Received message: ${message.value.toString()}`);
        messageReceived = true;
        console.log('✨ Kafka communication test PASSED!');
        await cleanup();
      },
    });

  } catch (error) {
    console.error('❌ Test failed with error:', error);
    process.exit(1);
  }
};

run();
