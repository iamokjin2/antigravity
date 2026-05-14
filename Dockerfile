FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Set entry point to news-scraper
CMD ["node", "scratch/news-scraper.js"]
