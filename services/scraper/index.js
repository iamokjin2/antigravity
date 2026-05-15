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
const NEW_TOPIC = 'news-newtopic';

const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:31379' });
redisClient.on('error', err => console.error('Redis Client Error', err));

const fetchNews = async () => {
    try {
        const response = await axios.get('https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=001', {
            responseType: 'arraybuffer',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Cache-Control': 'no-cache'
            }
        });

        const content = iconv.decode(Buffer.from(response.data), 'euc-kr');
        const $ = cheerio.load(content, { decodeEntities: false });
        const newsList = [];
        const items = $('.list_body ul li');

        for (let i = 0; i < items.length; i++) {
            const el = items[i];
            const titleEl = $(el).find('dl dt:not(.photo) a');
            const title = titleEl.text().trim();
            const link = titleEl.attr('href');
            const press = $(el).find('span.writing').text().trim();
            const thumbnail = $(el).find('dl dt.photo img').attr('src');

            if (title && press) {
                const fullLink = link.startsWith('http') ? link : `https://news.naver.com${link}`;
                let author = '';
                let articleContent = '';

                try {
                    const articleRes = await axios.get(fullLink, {
                        responseType: 'arraybuffer',
                        headers: {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                            'Referer': 'https://news.naver.com/'
                        },
                        timeout: 10000
                    });
                    const articleHtml = iconv.decode(Buffer.from(articleRes.data), 'utf-8');
                    const $article = cheerio.load(articleHtml);

                    author = $article('.byline_s').first().text().trim() || 
                             $article('.media_end_head_journalist_name').first().text().trim() ||
                             $article('.journalist_card_name').first().text().trim() ||
                             $article('em[class*="journalist"]').first().text().trim();
                    
                    if (author.includes('기자')) {
                        author = author.split('기자')[0] + '기자';
                    }
                    author = author.replace(/\s+/g, ' ').trim();

                    const contentEl = $article('#dic_area').length ? $article('#dic_area') : 
                                     ($article('#articleBodyContents').length ? $article('#articleBodyContents') : $article('.article_body'));
                    
                    if (contentEl.length) {
                        contentEl.find('script, style, span.end_photo_org, div.nbd_im_w, div.article_footer').remove();
                        articleContent = contentEl.text().trim().replace(/\s+/g, ' ').substring(0, 800) + '...';
                    }
                } catch (e) {
                    console.warn(`⚠️ Detail Fetch Failed [${fullLink}]: ${e.message}`);
                }

                newsList.push({
                    title,
                    link: fullLink,
                    press,
                    author: author || '기자 정보 없음',
                    content: articleContent || '본문 내용을 가져올 수 없습니다.',
                    thumbnail: thumbnail || '',
                    timestamp: new Date().toISOString()
                });
            }
        }
        return newsList;
    } catch (error) {
        console.error('❌ Scraping Error:', error.message);
        return [];
    }
};

const run = async () => {
    await producer.connect();
    await redisClient.connect();
    console.log('🚀 Naver News Scraper Producer started with Multi-Topic Production...');

    const scrapeAndSend = async () => {
        const news = await fetchNews();
        let sentCount = 0;

        for (const item of news) {
            const isDuplicate = await redisClient.get(`seen:${item.link}`);

            if (!isDuplicate) {
                const message = {
                    key: item.press,
                    value: JSON.stringify(item)
                };

                // Send to both topics
                await Promise.all([
                    producer.send({ topic: TOPIC, messages: [message] }),
                    producer.send({ topic: NEW_TOPIC, messages: [message] })
                ]);

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
