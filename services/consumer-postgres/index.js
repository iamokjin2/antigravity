const { Kafka } = require('kafkajs');
const { Client } = require('pg');
require('dotenv').config();

const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';
const BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'];
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'news-postgres-consumer-group';

const run = async () => {
    const kafka = new Kafka({
        clientId: 'news-consumer-postgres',
        brokers: BROKERS,
    });

    const pgClient = new Client({
        host: process.env.DB_HOST || 'localhost',
        port: process.env.DB_PORT || 31432,
        user: process.env.DB_USER || 'postgres',
        password: process.env.DB_PASSWORD || 'antigravity',
        database: process.env.DB_NAME || 'kafka_logs'
    });
    await pgClient.connect();

    // Ensure table exists
    await pgClient.query(`
        CREATE TABLE IF NOT EXISTS news_history (
            id SERIAL PRIMARY KEY,
            press VARCHAR(100),
            title TEXT,
            link TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    const consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    console.log('🐘 News Postgres Consumer started.');

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                await pgClient.query(
                    'INSERT INTO news_history (press, title, link) VALUES ($1, $2, $3)',
                    [data.press, data.title, data.link]
                );
                console.log(`✅ [Postgres] Inserted: ${data.title.substring(0, 30)}...`);
            } catch (err) {
                console.error('❌ Postgres Processing Error:', err.message);
            }
        },
    });
};

run().catch(console.error);
