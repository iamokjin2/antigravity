const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const { createClient } = require('redis');
const { Client } = require('pg');
const { Worker, isMainThread, workerData } = require('worker_threads');
require('dotenv').config();

const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
}

const TOPIC = 'news-topic';

// ---------------------------------------------------------------------------
// Main Thread Logic
// ---------------------------------------------------------------------------
if (isMainThread) {
    console.log('🚀 Main Thread: Initializing 10 News Consumer Threads...');

    const startWorkers = async () => {
        for (let i = 1; i <= 10; i++) {
            let type = 'FILE';
            if (i >= 5 && i <= 7) type = 'REDIS';
            if (i >= 8) type = 'POSTGRES';

            new Worker(__filename, {
                workerData: { id: i, type: type }
            });
            await new Promise(r => setTimeout(r, 500));
        }
        console.log('\n✨ All News Workers (4 File, 3 Redis, 3 Postgres) spawned.');
    };

    startWorkers().catch(console.error);

} 
// ---------------------------------------------------------------------------
// Worker Thread Logic
// ---------------------------------------------------------------------------
else {
    const { id, type } = workerData;

    const kafka = new Kafka({
        clientId: `news-worker-${id}`,
        brokers: ['localhost:31175'],
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

    // --- 📂 File Logging Worker ---
    async function runFileConsumer(workerId, kafkaInstance) {
        const logFilePath = path.join(logDir, `news-file-worker-${workerId}.txt`);
        const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
        const consumer = kafkaInstance.consumer({ groupId: `news-file-group-${workerId}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

        console.log(`📂 [Thread ${workerId}] File Consumer started.`);

        await consumer.run({
            eachMessage: async ({ partition, message }) => {
                const data = JSON.parse(message.value.toString());
                const logEntry = `[${new Date().toISOString()}] P:${partition} | [${data.press}] ${data.title}\n`;
                logStream.write(logEntry);
            },
        });
    }

    // --- 🗄️ Redis Logging Worker ---
    async function runRedisConsumer(workerId, kafkaInstance) {
        const redisClient = createClient({ url: 'redis://localhost:31379' });
        await redisClient.connect();
        const consumer = kafkaInstance.consumer({ groupId: `news-redis-group-${workerId}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

        console.log(`🗄️ [Thread ${workerId}] Redis Consumer started.`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    // Store latest news per press
                    await redisClient.hSet(`news:latest:${data.press}`, 'title', data.title);
                    await redisClient.hSet(`news:latest:${data.press}`, 'link', data.link);
                    await redisClient.hSet(`news:latest:${data.press}`, 'time', data.timestamp);
                } catch (err) {
                    console.error(`[Thread ${workerId}] Redis Error:`, err.message);
                }
            },
        });
    }

    // --- 🐘 Postgres Logging Worker ---
    async function runPostgresConsumer(workerId, kafkaInstance) {
        const pgClient = new Client({
            host: 'localhost',
            port: 31432,
            user: 'postgres',
            password: 'antigravity',
            database: 'kafka_logs'
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
