const axios = require('axios');
const cheerio = require('cheerio');
const iconv = require('iconv-lite');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({ 
    clientId: 'news-scraper', 
    brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:31175'] 
});
const producer = kafka.producer();
const TOPIC = process.env.KAFKA_TOPIC || 'news-topic';

const fetchNews = async () => {
    try {
        const response = await axios.get('https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=001', {
            responseType: 'arraybuffer',
            headers: { 
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Cache-Control': 'no-cache'
            }
        });
        
        // Use iconv-lite with 'euc-kr' (lowercase often works better in some envs)
        // and ensure we're dealing with a clean Buffer
        const content = iconv.decode(Buffer.from(response.data), 'euc-kr');
        
        // Disable entity decoding in cheerio to prevent double encoding issues
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
    console.log('🚀 Naver News Scraper Producer started (Ultra-Encoding-Fix)...');

    const scrapeAndSend = async () => {
        const news = await fetchNews();
        console.log(`📡 [${new Date().toLocaleTimeString()}] Scraping... Found ${news.length} articles.`);
        
        for (const item of news) {
            // Log one item to check encoding in terminal
            if (news.indexOf(item) === 0) {
                console.log(`📝 Sample Check -> Press: ${item.press}, Title: ${item.title.substring(0, 20)}...`);
            }

            await producer.send({
                topic: TOPIC,
                messages: [{ 
                    key: item.press, 
                    value: JSON.stringify(item) 
                }]
            });
        }
    };

    await scrapeAndSend();
    setInterval(scrapeAndSend, 10000);
};

run().catch(console.error);
