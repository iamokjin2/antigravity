const { Kafka } = require('kafkajs');
const axios = require('axios');
require('dotenv').config();

const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';
const BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'];
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'news-web-push-consumer-group';
const WEB_PUSH_SERVER_URL = process.env.WEB_PUSH_SERVER_URL || 'http://web-push-server:3000/push';

const run = async () => {
    const kafka = new Kafka({
        clientId: 'news-consumer-web-push',
        brokers: BROKERS,
    });

    const consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    console.log('🌐 News Web Push Consumer started.');

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                
                console.log(`📨 Pushing to Web Server: ${data.title.substring(0, 30)}...`);

                await axios.post(WEB_PUSH_SERVER_URL, {
                    press: data.press,
                    title: data.title
                });

            } catch (err) {
                console.error('❌ Web Push Processing Error:', err.message);
            }
        },
    });
};

run().catch(console.error);
