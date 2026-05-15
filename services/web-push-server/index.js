const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.json());

// HTML Client
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>News Push Service</title>
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
                .container { max-width: 800px; margin: 0 auto; }
                h1 { border-bottom: 2px solid #334155; padding-bottom: 10px; color: #38bdf8; }
                #news-list { list-style: none; padding: 0; }
                .news-item { background: #1e293b; margin-bottom: 15px; padding: 15px; border-radius: 8px; border-left: 5px solid #38bdf8; animation: slideIn 0.5s ease-out; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); }
                .news-item .press { font-size: 0.8rem; color: #94a3b8; font-weight: bold; text-transform: uppercase; }
                .news-item .title { font-size: 1.1rem; margin: 5px 0; color: #f1f5f9; }
                .news-item .time { font-size: 0.75rem; color: #64748b; text-align: right; }
                @keyframes slideIn { from { opacity: 0; transform: translateY(-20px); } to { opacity: 1; transform: translateY(0); } }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🚀 Real-time News Push</h1>
                <div id="status" style="color: #10b981; font-size: 0.8rem; margin-bottom: 10px;">Connected to server</div>
                <ul id="news-list"></ul>
            </div>
            <script src="/socket.io/socket.io.js"></script>
            <script>
                const socket = io();
                const newsList = document.getElementById('news-list');
                
                socket.on('news-push', (data) => {
                    const item = document.createElement('li');
                    item.className = 'news-item';
                    item.innerHTML = \`
                        <div class="press">\${data.press}</div>
                        <div class="title">\${data.title}</div>
                        <div class="time">\${new Date().toLocaleTimeString()}</div>
                    \`;
                    newsList.prepend(item);
                    if (newsList.children.length > 50) newsList.removeChild(newsList.lastChild);
                    
                    if (Notification.permission === "granted") {
                        new Notification(\`[\${data.press}] \${data.title}\`);
                    }
                });

                if (Notification.permission !== "denied") {
                    Notification.requestPermission();
                }
            </script>
        </body>
        </html>
    `);
});

// Endpoint for consumer to push news
app.post('/push', (req, res) => {
    const { press, title } = req.body;
    io.emit('news-push', { press, title });
    res.status(200).send('Pushed');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log('Web Server running on port ' + PORT);
});
