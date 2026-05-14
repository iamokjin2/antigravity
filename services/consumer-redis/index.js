const { Kafka } = require('kafkajs');
const { createClient } = require('redis');
require('dotenv').config();

const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';
const BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'];
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'news-redis-consumer-group';

const run = async () => {
    const kafka = new Kafka({
        clientId: 'news-consumer-redis',
        brokers: BROKERS,
    });

    const redisUrl = process.env.REDIS_URL || 'redis://localhost:31379';
    const redisClient = createClient({ url: redisUrl });
    await redisClient.connect();
    
    const consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    console.log('🗄️ News Redis Consumer started.');

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                await redisClient.hSet(`news:latest:${data.press}`, 'title', data.title);
                await redisClient.hSet(`news:latest:${data.press}`, 'link', data.link);
                await redisClient.hSet(`news:latest:${data.press}`, 'time', data.timestamp);
                console.log(`✅ [Redis] Stored: ${data.title.substring(0, 30)}...`);
            } catch (err) {
                console.error('❌ Redis Processing Error:', err.message);
            }
        },
    });
};

run().catch(console.error);
