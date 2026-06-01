import express from 'express';
import mediasoup from 'mediasoup';

const port = Number(process.env.PORT || 4000);
const rtcMinPort = Number(process.env.SFU_RTC_MIN_PORT || 14050);
const rtcMaxPort = Number(process.env.SFU_RTC_MAX_PORT || 14150);
const announcedIp = process.env.SFU_ANNOUNCED_IP || '127.0.0.1';

const mediaCodecs = [
  {
    kind: 'audio',
    mimeType: 'audio/opus',
    clockRate: 48000,
    channels: 2,
  },
];

const app = express();
app.use(express.json({ limit: '2mb' }));

const worker = await mediasoup.createWorker({
  rtcMinPort,
  rtcMaxPort,
  logLevel: 'warn',
  logTags: ['ice', 'dtls', 'rtp', 'srtp', 'rtcp'],
});

const rooms = new Map(); // channelId -> { router, peers: Map<sessionId, peer> }
const transportIndex = new Map(); // transportId -> { room, peer, transport }
const producerIndex = new Map(); // producerId -> { room, peer, producer }

function ensureRoom(channelId) {
  let room = rooms.get(channelId);
  if (room) return room;
  room = {
    router: null,
    peers: new Map(),
  };
  rooms.set(channelId, room);
  return room;
}

async function ensureRouter(room) {
  if (room.router) return room.router;
  room.router = await worker.createRouter({ mediaCodecs });
  return room.router;
}

function getPeer(sessionId) {
  for (const room of rooms.values()) {
    const peer = room.peers.get(sessionId);
    if (peer) return { room, peer };
  }
  return null;
}

function serializeIceCandidates(iceCandidates) {
  return iceCandidates.map((c) => ({
    foundation: c.foundation,
    priority: c.priority,
    ip: c.ip,
    address: c.address,
    protocol: c.protocol,
    port: c.port,
    type: c.type,
    tcpType: c.tcpType,
  }));
}

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'harbour-chat-sfu' });
});

app.post('/v1/sessions/bootstrap', async (req, res) => {
  try {
    const { userId, channelId } = req.body || {};
    if (!userId || !channelId) return res.status(400).json({ error: 'userId and channelId required' });

    const room = ensureRoom(channelId);
    const router = await ensureRouter(room);
    const sessionId = crypto.randomUUID();
    room.peers.set(sessionId, {
      sessionId,
      userId,
      channelId,
      transports: new Map(),
      producers: new Map(),
      consumers: new Map(),
    });
    res.json({
      session_id: sessionId,
      channel_id: channelId,
      user_id: userId,
      router_rtp_capabilities: router.rtpCapabilities,
    });
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'bootstrap failed' });
  }
});

app.delete('/v1/sessions/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  const found = getPeer(sessionId);
  if (!found) return res.status(204).end();
  const { room, peer } = found;
  for (const c of peer.consumers.values()) c.close();
  for (const p of peer.producers.values()) p.close();
  for (const t of peer.transports.values()) t.close();
  room.peers.delete(sessionId);
  if (room.peers.size === 0 && room.router) {
    room.router.close();
    room.router = null;
  }
  res.status(204).end();
});

app.post('/v1/transports', async (req, res) => {
  try {
    const { sessionId, direction } = req.body || {};
    const found = getPeer(sessionId);
    if (!found) return res.status(404).json({ error: 'session not found' });
    const { room, peer } = found;
    const router = await ensureRouter(room);
    const transport = await router.createWebRtcTransport({
      listenIps: [{ ip: '0.0.0.0', announcedIp }],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: 600000,
    });
    peer.transports.set(transport.id, transport);
    transportIndex.set(transport.id, { room, peer, transport });
    transport.on('close', () => transportIndex.delete(transport.id));
    res.json({
      session_id: sessionId,
      transport_id: transport.id,
      direction: direction || 'send',
      ice_parameters: transport.iceParameters,
      ice_candidates: serializeIceCandidates(transport.iceCandidates),
      dtls_parameters: transport.dtlsParameters,
    });
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'create transport failed' });
  }
});

app.post('/v1/transports/:transportId/connect', async (req, res) => {
  try {
    const { transportId } = req.params;
    const { dtlsParameters } = req.body || {};
    const found = transportIndex.get(transportId);
    if (!found) return res.status(404).json({ error: 'transport not found' });
    await found.transport.connect({ dtlsParameters });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'connect failed' });
  }
});

app.post('/v1/producers', async (req, res) => {
  try {
    const { sessionId, transportId, kind, rtpParameters } = req.body || {};
    const found = getPeer(sessionId);
    if (!found) return res.status(404).json({ error: 'session not found' });
    const transportFound = transportIndex.get(transportId);
    if (!transportFound) return res.status(404).json({ error: 'transport not found' });
    const producer = await transportFound.transport.produce({ kind, rtpParameters });
    found.peer.producers.set(producer.id, producer);
    producerIndex.set(producer.id, { room: found.room, peer: found.peer, producer });
    producer.on('transportclose', () => producerIndex.delete(producer.id));
    res.json({
      session_id: sessionId,
      producer_id: producer.id,
      transport_id: transportId,
      kind: producer.kind,
    });
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'create producer failed' });
  }
});

app.get('/v1/sessions/:sessionId/producers', async (req, res) => {
  const { sessionId } = req.params;
  const found = getPeer(sessionId);
  if (!found) return res.status(404).json({ error: 'session not found' });
  const out = [];
  for (const [otherSessionId, otherPeer] of found.room.peers.entries()) {
    if (otherSessionId === sessionId) continue;
    for (const producer of otherPeer.producers.values()) {
      out.push({ producer_id: producer.id, kind: producer.kind, user_id: otherPeer.userId });
    }
  }
  res.json({ producers: out });
});

app.post('/v1/consumers', async (req, res) => {
  try {
    const { sessionId, transportId, producerId, rtpCapabilities } = req.body || {};
    const found = getPeer(sessionId);
    if (!found) return res.status(404).json({ error: 'session not found' });
    const producerFound = producerIndex.get(producerId);
    if (!producerFound) return res.status(404).json({ error: 'producer not found' });
    if (!found.room.router.canConsume({ producerId, rtpCapabilities })) {
      return res.status(400).json({ error: 'cannot consume producer' });
    }
    const transportFound = transportIndex.get(transportId);
    if (!transportFound) return res.status(404).json({ error: 'transport not found' });
    const consumer = await transportFound.transport.consume({
      producerId,
      rtpCapabilities,
      paused: false,
    });
    found.peer.consumers.set(consumer.id, consumer);
    consumer.on('transportclose', () => found.peer.consumers.delete(consumer.id));
    res.json({
      session_id: sessionId,
      consumer_id: consumer.id,
      producer_id: producerId,
      transport_id: transportId,
      kind: consumer.kind,
      rtp_parameters: consumer.rtpParameters,
    });
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'create consumer failed' });
  }
});

app.post('/v1/transports/:transportId/restart-ice', async (req, res) => {
  try {
    const { transportId } = req.params;
    const found = transportIndex.get(transportId);
    if (!found) return res.status(404).json({ error: 'transport not found' });
    const ice = await found.transport.restartIce();
    res.json(ice);
  } catch (e) {
    res.status(500).json({ error: e instanceof Error ? e.message : 'restart ice failed' });
  }
});

app.post('/v1/transports/:transportId/ice-candidates', (_req, res) => {
  // mediasoup WebRtcTransport does not require explicit server-side candidate injection
  res.json({ ok: true });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`harbour-chat-sfu listening on :${port}`);
});
