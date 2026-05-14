const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'load-generator',
  brokers: ['localhost:31175']
});

const producer = kafka.producer();

const run = async () => {
  const topic = 'test-topic';
  const TOTAL_MESSAGES = 5000;
  const BATCH_SIZE = 100;

  console.log(`🚀 Starting load test: Generating ${TOTAL_MESSAGES} messages...`);
  await producer.connect();

  const startTime = Date.now();
  let sentCount = 0;

  try {
    for (let i = 0; i < TOTAL_MESSAGES / BATCH_SIZE; i++) {
      const messages = [];
      for (let j = 0; j < BATCH_SIZE; j++) {
        const id = i * BATCH_SIZE + j;
        messages.push({
          key: `item-${id}`,
          value: JSON.stringify({
            id: id,
            item: `Product ${id}`,
            price: Math.floor(Math.random() * 100000),
            region: ['Seoul', 'Busan', 'Incheon', 'Daegu', 'Gwangju'][Math.floor(Math.random() * 5)],
            category: ['Electronics', 'Fashion', 'Home', 'Hobby'][Math.floor(Math.random() * 4)],
            timestamp: new Date().toISOString()
          })
        });
      }

      await producer.send({
        topic,
        messages: messages,
      });
      
      sentCount += BATCH_SIZE;
      if (sentCount % 1000 === 0) {
        console.log(`📡 Progress: ${sentCount}/${TOTAL_MESSAGES} messages sent...`);
      }
    }

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n✨ Load Test Completed!`);
    console.log(`-----------------------------------`);
    console.log(`Total Messages : ${sentCount}`);
    console.log(`Total Time     : ${duration.toFixed(2)}s`);
    console.log(`Throughput     : ${Math.floor(sentCount / duration)} msg/sec`);
    console.log(`-----------------------------------`);

  } catch (error) {
    console.error('❌ Load generation failed:', error);
  } finally {
    await producer.disconnect();
    process.exit(0);
  }
};

run();
