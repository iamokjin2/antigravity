const { Kafka } = require('kafkajs');
const axios = require('axios');
require('dotenv').config();

const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';
const BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'];
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'news-n8n-consumer-group';
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || 'http://n8n:5678/webhook/3172806b-8f68-448c-8458-4aa0ab18e4c0';

const run = async () => {
    const kafka = new Kafka({
        clientId: 'news-consumer-n8n',
        brokers: BROKERS,
    });

    const consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    console.log('🚀 News n8n Consumer started.');

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                const newsTitle = data.title;

                console.log(`📡 Sending to n8n: ${newsTitle.substring(0, 30)}...`);

                await axios.post(N8N_WEBHOOK_URL, {
                    message: newsTitle
                });

                console.log(`✅ [n8n] Successfully sent: ${newsTitle.substring(0, 30)}...`);
            } catch (err) {
                console.error('❌ n8n Processing Error:', err.response ? err.response.data : err.message);
            }
        },
    });
};

run().catch(console.error);
