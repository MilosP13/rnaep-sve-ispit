const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Server } = require('socket.io'); // Dodajte Server iz socket.io

const app = express();
const port = 8000;

app.use(cors());
app.use(bodyParser.json());

// HTTP server
const server = http.createServer(app);

// WebSocket server
const io = new Server(server, {
  cors: {
    origin: 'http://localhost:3000', // Dodajte URL vašeg React klijenta
    methods: ['GET', 'POST'],
  },
});


// Array za čuvanje konektovanih klijenata
const clients = new Set();

// ***** ovo sve iznad je kao api koji ti otvaras da react dodje i uzme podatke, a na dole je isto to samo za consumera *****

// Endpoint za primanje podataka od Kafka consumera
app.post('/api/receive-data', (req, res) => {
    const receivedData = req.body;

    // Slanje podataka svim klijentima putem WebSocket-a
    const jsonData = JSON.stringify(receivedData);
    clients.forEach(client => {
        client.send(jsonData);
    });

    // Send a response
    res.status(200).send('Data received successfully');
});

// WebSocket handler
io.on('connection', (socket) => {
    console.log('New WebSocket connection');

    // Dodaj novog klijenta u listu
    clients.add(socket);

    // Kada klijent zatvori vezu, ukloni ga iz liste
    socket.on('disconnect', () => {
        console.log('WebSocket connection closed');
        clients.delete(socket);
    });
});

// Start the server
server.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});