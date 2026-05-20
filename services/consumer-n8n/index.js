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

    const randomGroupId = `${GROUP_ID}-${Math.random().toString(36).substring(2, 10)}`;
    const consumer = kafka.consumer({ groupId: randomGroupId });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    console.log('🚀 News n8n Consumer started.');

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const rawValue = message.value.toString();
                console.log(`📦 Raw message: ${rawValue}`);
                const data = JSON.parse(rawValue);
                console.log(`🔍 Parsed data:`, data);
                
                const newsTitle = data.title;
                const newsTime = data.timestamp;
                const newsPress = data.press;
                const sentTime = new Date().toISOString();

                const formattedMessage = `📢 [${newsPress}] ${newsTitle}\n⏰ 수집시간: ${sentTime}\n🕒 뉴스시간: ${newsTime}`;

                console.log(`📡 Sending to n8n: ${newsTitle.substring(0, 30)}...`);

                await axios.post(N8N_WEBHOOK_URL, {
                    message: formattedMessage
                });

                console.log(`✅ [n8n] Successfully sent: ${newsTitle.substring(0, 30)}...`);
            } catch (err) {
                console.error('❌ n8n Processing Error:', err.response ? err.response.data : err.message);
            }
        },
    });
};

run().catch(console.error);
