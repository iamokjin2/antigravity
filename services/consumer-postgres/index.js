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

    // Ensure table exists with new columns
    await pgClient.query(`
        CREATE TABLE IF NOT EXISTS news_history (
            id SERIAL PRIMARY KEY,
            press VARCHAR(100),
            title TEXT,
            link TEXT,
            author VARCHAR(100),
            content TEXT,
            thumbnail TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    // Migration: Add columns if they don't exist
    const columns = ['author', 'content', 'thumbnail'];
    for (const col of columns) {
        await pgClient.query(`
            DO $$ 
            BEGIN 
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='news_history' AND column_name='${col}') THEN
                    ALTER TABLE news_history ADD COLUMN ${col} TEXT;
                END IF;
            END $$;
        `);
    }

    const consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    console.log('🐘 News Postgres Consumer started.');

    let firstLogDone = false;

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                
                if (!firstLogDone) {
                    console.log('📦 [Postgres] Received Data Sample:');
                    console.log(JSON.stringify(data, null, 2));
                    firstLogDone = true;
                }

                await pgClient.query(
                    'INSERT INTO news_history (press, title, link, author, content, thumbnail) VALUES ($1, $2, $3, $4, $5, $6)',
                    [data.press, data.title, data.link, data.author || '', data.content || '', data.thumbnail || '']
                );
                console.log(`✅ [Postgres] Inserted: ${data.title.substring(0, 30)}...`);
            } catch (err) {
                console.error('❌ Postgres Processing Error:', err.message);
            }
        },
    });
};

run().catch(console.error);
