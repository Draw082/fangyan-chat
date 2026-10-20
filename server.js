const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e7 });

app.use(express.static(path.join(__dirname, 'public')));

// 房间数据：
// rooms: Map<roomId, { users: Map<socketId, user>, hostUserId: string, messages: [] }>
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

    // 如果房间不存在，则新建（这就实现了“解散后再次输入视为新房间”）
    if (!rooms.has(roomId)) {
      rooms.set(roomId, { 
        users: new Map(), 
        messages: [], 
        hostUserId: userId
      });
    }
    
    const room = rooms.get(roomId);
    
    // 如果用户是断线重连或刷新，把旧的 socket 记录移除，保留他的历史状态
    for (const [oldSocketId, u] of room.users.entries()) {
      if (u.userId === userId) {
        room.users.delete(oldSocketId);
        // 踢掉旧的连接（如果有的话）
        io.sockets.sockets.get(oldSocketId)?.disconnect();
      }
    }

    const user = { id: socket.id, userId, name };
    room.users.set(socket.id, user);

    // 如果原主持人不在房间里，或者还没有主持人，当前用户自动接管
    if (!room.hostUserId || !Array.from(room.users.values()).some(u => u.userId === room.hostUserId)) {
      room.hostUserId = userId;
    }

    // 给该用户发送初始化数据
    socket.emit('room:init', {
      users: Array.from(room.users.values()),
      hostUserId: room.hostUserId,
      messages: room.messages,
      myUserId: userId
    });

    // 广播给房间内其他人
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
      from: user.userId, // 用持久化的 userId 发送
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
      // 私聊：参与人使用 userId 集合
      const recipients = Array.isArray(msg.to) ? msg.to : [msg.to];
      const participants = Array.from(new Set([...recipients, user.userId])).sort();
      newMsg.to = participants;
      
      room.messages.push(newMsg);

      // 只发给对应 userId 的 socket
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
    // 只有主持人能解散
    if (room && currentUserId === room.hostUserId) {
      rooms.delete(currentRoomId); // 彻底销毁房间，后续同房间码会重新创建
      io.to(currentRoomId).emit('room:dissolved');
    }
  });

  // 断开连接（关闭网页、刷新、退出）
  socket.on('disconnect', () => {
    if (!currentRoomId) return;
    const room = rooms.get(currentRoomId);
    if (!room) return;
    
    room.users.delete(socket.id);
    
    // 如果主持人断开了，自动转让给房间内剩下的人
    if (currentUserId === room.hostUserId) {
      const remainingUsers = Array.from(room.users.values());
      if (remainingUsers.length > 0) {
        room.hostUserId = remainingUsers[0].userId;
        io.to(currentRoomId).emit('host:changed', room.hostUserId);
        io.to(currentRoomId).emit('system:msg', `主持人已离开，自动转移给 ${remainingUsers[0].name}`);
      } else {
        room.hostUserId = null; // 房间空了
      }
    }
    
    io.to(currentRoomId).emit('room:update-users', Array.from(room.users.values()));
  });
});

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});