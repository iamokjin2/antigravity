const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const { createClient } = require('redis');
const { Client } = require('pg');
const { Worker, isMainThread, workerData } = require('worker_threads');
require('dotenv').config();

const logDir = process.env.LOG_DIR || path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';
const BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'];

if (isMainThread) {
    console.log('🚀 Main Thread: Initializing News Consumer Threads...');
    const workerCount = parseInt(process.env.WORKER_COUNT) || 10;

    const startWorkers = async () => {
        for (let i = 1; i <= workerCount; i++) {
            let type = 'FILE';
            if (i >= 5 && i <= 7) type = 'REDIS';
            if (i >= 8) type = 'POSTGRES';

            new Worker(__filename, {
                workerData: { id: i, type: type }
            });
            await new Promise(r => setTimeout(r, 500));
        }
        console.log(`\n✨ All ${workerCount} News Workers spawned.`);
    };

    startWorkers().catch(console.error);

} else {
    const { id, type } = workerData;
    const kafka = new Kafka({
        clientId: `news-worker-${id}`,
        brokers: BROKERS,
    });

    const run = async () => {
        if (type === 'FILE') {
            await runFileConsumer(id, kafka);
        } else if (type === 'REDIS') {
            await runRedisConsumer(id, kafka);
        } else if (type === 'POSTGRES') {
            await runPostgresConsumer(id, kafka);
        }
    };

    async function runFileConsumer(workerId, kafkaInstance) {
        const logFilePath = path.join(logDir, `news-file-worker-${workerId}.txt`);
        const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
        const consumer = kafkaInstance.consumer({ groupId: `news-file-group-${workerId}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

        console.log(`📂 [Thread ${workerId}] File Consumer started.`);

        await consumer.run({
            eachMessage: async ({ partition, message }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    const logEntry = `[${new Date().toISOString()}] P:${partition} | [${data.press}] ${data.title}\n`;
                    logStream.write(logEntry);
                } catch (err) {
                    console.error(`[Thread ${workerId}] File Parse Error:`, err.message);
                }
            },
        });
    }

    async function runRedisConsumer(workerId, kafkaInstance) {
        const redisUrl = process.env.REDIS_URL || 'redis://localhost:31379';
        const redisClient = createClient({ url: redisUrl });
        await redisClient.connect();
        const consumer = kafkaInstance.consumer({ groupId: `news-redis-group-${workerId}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

        console.log(`🗄️ [Thread ${workerId}] Redis Consumer started.`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    await redisClient.hSet(`news:latest:${data.press}`, 'title', data.title);
                    await redisClient.hSet(`news:latest:${data.press}`, 'link', data.link);
                    await redisClient.hSet(`news:latest:${data.press}`, 'time', data.timestamp);
                } catch (err) {
                    console.error(`[Thread ${workerId}] Redis Error:`, err.message);
                }
            },
        });
    }

    async function runPostgresConsumer(workerId, kafkaInstance) {
        const pgClient = new Client({
            host: process.env.DB_HOST || 'localhost',
            port: process.env.DB_PORT || 31432,
            user: process.env.DB_USER || 'postgres',
            password: process.env.DB_PASSWORD || 'antigravity',
            database: process.env.DB_NAME || 'kafka_logs'
        });
        await pgClient.connect();

        await pgClient.query(`
            CREATE TABLE IF NOT EXISTS news_history (
                id SERIAL PRIMARY KEY,
                worker_id INT,
                press VARCHAR(100),
                title TEXT,
                link TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        const consumer = kafkaInstance.consumer({ groupId: `news-pg-group-${workerId}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

        console.log(`🐘 [Thread ${workerId}] Postgres Consumer started.`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    await pgClient.query(
                        'INSERT INTO news_history (worker_id, press, title, link) VALUES ($1, $2, $3, $4)',
                        [workerId, data.press, data.title, data.link]
                    );
                } catch (err) {
                    console.error(`[Thread ${workerId}] Postgres Error:`, err.message);
                }
            },
        });
    }

    run().catch(err => {
        console.error(`❌ [Thread ${id}] Crashed:`, err);
    });
}
