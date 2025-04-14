// server.js

import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import dotenv from 'dotenv';
dotenv.config();

import { RealtimeClient } from '@openai/realtime-api-beta';

// Express setup
const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Set up view engine and static files
app.set('view engine', 'ejs');
app.set('views', './views');
app.use(express.static('public'));

// Main Route
app.get('/', (req, res) => {
    res.render('index');
  });

// Socket.io setup
io.on('connection', async (socket) => { // Make handler async
    console.log(`Socket connected: ${socket.id}`);
    const client = new RealtimeClient({ apiKey: process.env.OPENAI_API_KEY });
    let isClientConnected = false; // Flag to track connection status

    client.updateSession({
        instructions: 'You are a helpful, english speaking assistant.',
        voice: 'alloy',
        turn_detection: { type: 'server_vad', threshold: 0.3 },
        output_audio: { model: 'audio-davinci', format: 'pcm' },
        input_audio_transcription: { model: 'whisper-1' },
    });

    // --- Client Event Listeners (can be attached before connect) ---
    client.on('error', (error) => {
        console.error(`Realtime API error for socket ${socket.id}:`, error);
        socket.emit('error', 'Realtime API error occurred.'); // Inform client
        isClientConnected = false; // Assume connection is lost on error
    });

    // Handle conversation updates for transcription and audio
    client.on('conversation.updated', (event) => {
        const { item, delta } = event;

        // Handle user input (partial or complete transcription)
        if (item.role === 'user' && item.formatted.transcript) {
            socket.emit('displayUserMessage', {
                text: item.formatted.transcript,
                isFinal: item.status === 'completed',
            });
        } else if (item.role === 'user' && item.formatted.audio?.length && !item.formatted.transcript) {
            // Emit placeholder while waiting for transcript if audio is present
            socket.emit('displayUserMessage', {
                text: "(awaiting transcript)",
                isFinal: false
            });
        } else if (item.role === 'user' && !item.formatted.transcript) {
            // Fallback in case neither transcript nor audio is present
            socket.emit('displayUserMessage', {
                text: "(item sent)",
                isFinal: true
            });
        }

        // Send bot responses to the client
        if (item.role !== 'user' && item.formatted.transcript) {
            socket.emit('conversationUpdate', {
                text: item.formatted.transcript,
                isFinal: item.status === 'completed',
            });
        }

        // Send audio updates to client
        if (delta?.audio) {
            const audioData = delta.audio.buffer || delta.audio;
            socket.emit('audioStream', audioData, item.id);
        }
    });

     // Handle conversation interruption
    client.on('conversation.interrupted', async () => {
        // No need to check isClientConnected here, just forward to client
        socket.emit('conversationInterrupted');
    });

    // --- Socket Event Listeners (Attached outside connect attempt) ---

    // Handle incoming audio data from the client
    socket.on('audioInput', async (data) => {
        if (!isClientConnected) {
            console.warn(`Socket ${socket.id}: Received audioInput but RealtimeClient is not connected. Ignoring.`);
            return;
        }
        if (data) {
            try {
                const buffer = new Uint8Array(data).buffer;
                const int16Array = new Int16Array(buffer);
                await client.appendInputAudio(int16Array);
            } catch (error) {
                console.error(`Socket ${socket.id}: Error processing audio data:`, error);
            }
        }
    });

    // Handle cancel response requests from the client
    socket.on('cancelResponse', async ({ trackId, offset }) => {
        if (!isClientConnected) {
             console.warn(`Socket ${socket.id}: Received cancelResponse but RealtimeClient is not connected. Ignoring.`);
            return;
        }
        if (trackId) {
            try {
                await client.cancelResponse(trackId, offset);
            } catch (error) {
                console.error(`Socket ${socket.id}: Error canceling response:`, error);
            }
        }
    });

    // Handle text messages from the user
    socket.on('userMessage', (message) => {
         if (!isClientConnected) {
             console.warn(`Socket ${socket.id}: Received userMessage but RealtimeClient is not connected. Ignoring.`);
            return;
         }
        client.sendUserMessageContent([{ type: 'input_text', text: message }]);
    });

    socket.on('disconnect', () => {
        console.log(`Socket disconnected: ${socket.id}`);
        if (isClientConnected) {
            client.disconnect();
            isClientConnected = false;
        }
    });

    // --- Attempt Connection ---
    try {
        await client.connect();
        console.log(`RealtimeClient connected successfully for socket ${socket.id}`);
        isClientConnected = true; // Set flag on successful connection
    } catch (error) {
        console.error(`Failed to connect RealtimeClient for socket ${socket.id}:`, error);
        socket.emit('error', 'Failed to connect to OpenAI API.');
        // isClientConnected remains false
    }
});

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});
