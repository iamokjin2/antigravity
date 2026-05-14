FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
# Create logs directory
RUN mkdir -p /app/logs
CMD ["node", "index.js"]
