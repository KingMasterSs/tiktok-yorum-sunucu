import { WebSocketServer } from 'ws';
import { TikTokLiveConnection } from 'tiktok-live-connector';

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });
const rooms = new Map();

function broadcastToRoom(cleanUser, payload) {
  const room = rooms.get(cleanUser);
  if (!room) return;
  const rawData = JSON.stringify(payload);
  for (const client of room.clients) {
    if (client.readyState === 1) {
      client.send(rawData);
    }
  }
}

function getOrCreateRoom(rawUsername) {
  const cleanUser = rawUsername.trim().replace(/^@/, '').toLowerCase();
  if (!cleanUser) return null;

  if (rooms.has(cleanUser)) {
    return rooms.get(cleanUser);
  }

  const roomState = {
    clients: new Set(),
    connection: new TikTokLiveConnection(cleanUser),
    isConnected: false
  };

  rooms.set(cleanUser, roomState);

  roomState.connection.connect().then(() => {
    roomState.isConnected = true;
    broadcastToRoom(cleanUser, {
      type: 'status',
      message: `@${cleanUser} canlı yayınına bağlanıldı.`
    });
  }).catch(() => {
    roomState.isConnected = false;
    broadcastToRoom(cleanUser, {
      type: 'error',
      message: `@${cleanUser} yayınına bağlanılamadı (yayında olmayabilir).`
    });
    rooms.delete(cleanUser);
  });

  roomState.connection.on('chat', (data) => {
    broadcastToRoom(cleanUser, {
      type: 'chat',
      user: data?.uniqueId || 'bilinmeyen',
      comment: data?.comment || '',
      timestamp: Date.now()
    });
  });

  roomState.connection.on('disconnected', () => {
    roomState.isConnected = false;
    broadcastToRoom(cleanUser, {
      type: 'status',
      message: `@${cleanUser} canlı yayın bağlantısı sonlandı.`
    });
  });

  roomState.connection.on('error', () => {
    broadcastToRoom(cleanUser, {
      type: 'error',
      message: `@${cleanUser} yayın akışında bağlantı hatası oluştu.`
    });
  });

  return roomState;
}

wss.on('connection', (ws) => {
  let currentRoomKey = null;

  ws.on('message', (rawMessage) => {
    try {
      const parsed = JSON.parse(rawMessage.toString());
      if (parsed.action === 'join' && typeof parsed.username === 'string') {
        const cleanUser = parsed.username.trim().replace(/^@/, '').toLowerCase();
        if (!cleanUser) {
          ws.send(JSON.stringify({ type: 'error', message: 'Geçerli bir kullanıcı adı girin.' }));
          return;
        }

        if (currentRoomKey && rooms.has(currentRoomKey)) {
          const prevRoom = rooms.get(currentRoomKey);
          prevRoom.clients.delete(ws);
          if (prevRoom.clients.size === 0) {
            prevRoom.connection.disconnect().catch(() => {});
            rooms.delete(currentRoomKey);
          }
        }

        const room = getOrCreateRoom(cleanUser);
        room.clients.add(ws);
        currentRoomKey = cleanUser;

        ws.send(JSON.stringify({
          type: 'status',
          message: `@${cleanUser} odasına katıldınız, yayın kontrol ediliyor...`
        }));
      }
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Geçersiz JSON mesajı.' }));
    }
  });

  ws.on('close', () => {
    if (currentRoomKey && rooms.has(currentRoomKey)) {
      const room = rooms.get(currentRoomKey);
      room.clients.delete(ws);
      if (room.clients.size === 0) {
        room.connection.disconnect().catch(() => {});
        rooms.delete(currentRoomKey);
      }
    }
  });
});

console.log(`Ücretsiz TikTok yorum sunucusu ${PORT} portunda çalışıyor...`);
