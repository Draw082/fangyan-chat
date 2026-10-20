const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e7 });

app.use(express.static(path.join(__dirname, 'public')));

// 内存数据库
const rooms = new Map();

io.on('connection', (socket) => {
  let currentRoomId = null;
  let currentUserId = null;

  // 用户进房
  socket.on('user:join', ({ roomId, name, userId }) => {
    if (!userId) {
      userId = 'u_' + Date.now() + Math.random().toString(36).slice(2, 8);
    }
    
    currentRoomId = roomId;
    currentUserId = userId;
    socket.join(roomId);

    if (!rooms.has(roomId)) {
      rooms.set(roomId, { 
        users: new Map(), 
        messages: [], 
        hostUserId: userId
      });
    }
    
    const room = rooms.get(roomId);
    
    // 断线重连处理：移除旧 socket，保留历史状态
    for (const [oldSocketId, u] of room.users.entries()) {
      if (u.userId === userId) {
        room.users.delete(oldSocketId);
        io.sockets.sockets.get(oldSocketId)?.disconnect();
      }
    }

    const user = { id: socket.id, userId, name };
    room.users.set(socket.id, user);

    if (!room.hostUserId || !Array.from(room.users.values()).some(u => u.userId === room.hostUserId)) {
      room.hostUserId = userId;
    }

    socket.emit('room:init', {
      users: Array.from(room.users.values()),
      hostUserId: room.hostUserId,
      messages: room.messages,
      myUserId: userId
    });

    io.to(roomId).emit('room:update-users', Array.from(room.users.values()));
    io.to(roomId).emit('host:changed', room.hostUserId);
  });

  // 收到消息
  socket.on('chat:send', (msg) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    const user = room.users.get(socket.id);
    if (!user) return;

    const newMsg = {
      id: 'msg_' + Date.now() + Math.random().toString(16).slice(2, 6),
      from: user.userId,
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
      const recipients = Array.isArray(msg.to) ? msg.to : [msg.to];
      const participants = Array.from(new Set([...recipients, user.userId])).sort();
      newMsg.to = participants;
      
      room.messages.push(newMsg);

      room.users.forEach((u, sId) => {
        if (participants.includes(u.userId)) {
          io.to(sId).emit('chat:receive', newMsg);
        }
      });
    }
  });

  // 撤回消息
  socket.on('chat:recall', ({ msgId }) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    const msg = room.messages.find(m => m.id === msgId);
    if (msg && msg.from === currentUserId) {
      msg.isRecalled = true;
      msg.content = null;
      msg.duration = null;
      io.to(currentRoomId).emit('chat:recalled', { msgId });
    }
  });

  // 转移主持人
  socket.on('host:transfer', (targetUserId) => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    
    if (currentUserId === room.hostUserId) {
      const targetUser = Array.from(room.users.values()).find(u => u.userId === targetUserId);
      if (targetUser) {
        room.hostUserId = targetUserId;
        io.to(currentRoomId).emit('host:changed', targetUserId);
        io.to(currentRoomId).emit('system:msg', `主持人已转移给 ${targetUser.name}`);
      }
    }
  });

  // 解散聊天室
  socket.on('room:dissolve', () => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (room && currentUserId === room.hostUserId) {
      // 1. 彻底删除房间数据
      rooms.delete(currentRoomId); 
      // 2. 通知所有人房间已解散
      io.to(currentRoomId).emit('room:dissolved');
      // 3. 把所有连接踢出该房间频道，避免后续触发“主持人离开”的广播
      io.in(currentRoomId).socketsLeave(currentRoomId);
    }
  });

  // 断开连接
  socket.on('disconnect', () => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    
    room.users.delete(socket.id);
    
    if (currentUserId === room.hostUserId) {
      const remainingUsers = Array.from(room.users.values());
      if (remainingUsers.length > 0) {
        room.hostUserId = remainingUsers[0].userId;
        io.to(currentRoomId).emit('host:changed', room.hostUserId);
        io.to(currentRoomId).emit('system:msg', `主持人已离开，自动转移给 ${remainingUsers[0].name}`);
      } else {
        room.hostUserId = null;
      }
    }
    
    io.to(currentRoomId).emit('room:update-users', Array.from(room.users.values()));
  });
});

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});