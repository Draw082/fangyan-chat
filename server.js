const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e7 });

app.use(express.static(path.join(__dirname, 'public')));

// 内存数据库：按房间码隔离数据
const rooms = new Map(); // roomId -> { users: Map, messages: [], hostId: null }

io.on('connection', (socket) => {
  let currentRoomId = null;

  // 用户进房
  socket.on('user:join', ({ roomId, name }) => {
    currentRoomId = roomId;
    socket.join(roomId);

    if (!rooms.has(roomId)) {
      rooms.set(roomId, { users: new Map(), messages: [], hostId: null });
    }
    const room = rooms.get(roomId);

    const user = { id: socket.id, name };
    room.users.set(socket.id, user);
    
    // 如果房间里没有主持人，或者原主持人已离开，则自己成为主持人
    if (!room.hostId || !room.users.has(room.hostId)) {
      room.hostId = socket.id;
    }

    // 把最新在线列表、主持人、历史记录发给该用户
    socket.emit('room:init', {
      users: Array.from(room.users.values()),
      hostId: room.hostId,
      messages: room.messages
    });

    // 广播给该房间内其他人
    socket.to(roomId).emit('room:update-users', Array.from(room.users.values()));
    io.to(roomId).emit('host:changed', room.hostId);
  });

  // 收到消息
  socket.on('chat:send', (msg) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    const user = room.users.get(socket.id);
    if (!user) return;

    const newMsg = {
      id: 'msg_' + Date.now() + Math.random().toString(16).slice(2, 6),
      from: user.id,
      fromName: user.name,
      type: msg.type,
      content: msg.content,
      duration: msg.duration,
      time: Date.now(),
      isRecalled: false,
      isPrivate: msg.to !== 'public'
    };

    if (msg.to === 'public') {
      newMsg.to = 'public';
      room.messages.push(newMsg);
      io.to(currentRoomId).emit('chat:receive', newMsg);
    } else {
      // 私聊：确保 to 字段包含所有参与者（含发送者自己），并按字母排序，方便前端生成一致的会话Key
      const recipients = Array.isArray(msg.to) ? msg.to : [msg.to];
      const participants = Array.from(new Set([...recipients, socket.id])).sort();
      newMsg.to = participants;
      
      room.messages.push(newMsg);

      // 只发给对应的人
      participants.forEach(id => {
        const targetSocket = io.sockets.sockets.get(id);
        if (targetSocket) {
          targetSocket.emit('chat:receive', newMsg);
        }
      });
    }
  });

  // 撤回消息
  socket.on('chat:recall', ({ msgId }) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    const msg = room.messages.find(m => m.id === msgId);
    if (msg && msg.from === socket.id) {
      msg.isRecalled = true;
      msg.content = null;
      msg.duration = null;
      io.to(currentRoomId).emit('chat:recalled', { msgId });
    }
  });

  // 转移主持人
  socket.on('host:transfer', (targetId) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (socket.id === room.hostId && room.users.has(targetId)) {
      room.hostId = targetId;
      io.to(currentRoomId).emit('host:changed', targetId);
      io.to(currentRoomId).emit('system:msg', `主持人已转移给 ${room.users.get(targetId).name}`);
    }
  });

  // 断开连接
  socket.on('disconnect', () => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    
    room.users.delete(socket.id);
    if (room.hostId === socket.id) {
      room.hostId = room.users.keys().next().value || null;
    }
    
    io.to(currentRoomId).emit('room:update-users', Array.from(room.users.values()));
    io.to(currentRoomId).emit('host:changed', room.hostId);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`服务器运行在 http://localhost:${PORT}`);
});