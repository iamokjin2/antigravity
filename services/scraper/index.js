const axios = require('axios');
const cheerio = require('cheerio');
const iconv = require('iconv-lite');
const { Kafka } = require('kafkajs');
const { createClient } = require('redis');
require('dotenv').config();

const kafka = new Kafka({
    clientId: 'news-scraper',
    brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175']
});
const producer = kafka.producer();
const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';

const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:31379' });
redisClient.on('error', err => console.error('Redis Client Error', err));

const fetchNews = async () => {
    try {
        const response = await axios.get('https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=001', {
            responseType: 'arraybuffer',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Cache-Control': 'no-cache'
            }
        });

        const content = iconv.decode(Buffer.from(response.data), 'euc-kr');
        const $ = cheerio.load(content, { decodeEntities: false });
        const newsList = [];

        $('.list_body ul li').each((i, el) => {
            const titleEl = $(el).find('dl dt:not(.photo) a');
            const title = titleEl.text().trim();
            const link = titleEl.attr('href');
            const press = $(el).find('span.writing').text().trim();

            if (title && press) {
                newsList.push({
                    title: title,
                    link: link.startsWith('http') ? link : `https://news.naver.com${link}`,
                    press: press,
                    timestamp: new Date().toISOString()
                });
            }
        });
        return newsList;
    } catch (error) {
        console.error('❌ Scraping Error:', error.message);
        return [];
    }
};

const run = async () => {
    await producer.connect();
    await redisClient.connect();
    console.log('🚀 Naver News Scraper Producer started with Duplicate Detection...');

    const scrapeAndSend = async () => {
        const news = await fetchNews();
        let sentCount = 0;

        for (const item of news) {
            // Check if link already exists in Redis
            const isDuplicate = await redisClient.get(`seen:${item.link}`);

            if (!isDuplicate) {
                await producer.send({
                    topic: TOPIC,
                    messages: [{
                        key: item.press,
                        value: JSON.stringify(item)
                    }]
                });

                // Mark as seen for 24 hours
                await redisClient.set(`seen:${item.link}`, 'true', { EX: 86400 });
                sentCount++;
            }
        }
        console.log(`📡 [${new Date().toLocaleTimeString()}] Scraping: Found ${news.length}, New: ${sentCount}`);
    };

    await scrapeAndSend();
    setInterval(scrapeAndSend, 20000);
};

run().catch(console.error);
