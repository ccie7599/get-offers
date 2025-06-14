const { connect, StringCodec, consumerOpts } = require('nats');
const express = require('express');
const fs = require('fs');
const path = require('path');
const cors = require('cors');

const DATA_DIR = '/tmp/data';
const NATS_SERVER = 'localhost:4222';
const SUBJECT = 'publish-offers.*';
const STREAM = 'publish-offers-stream';

async function ensureStreamExists(jsm) {
  try {
    await jsm.streams.info(STREAM);
    console.log(`✅ Stream "${STREAM}" already exists.`);
  } catch (err) {
    if (err.code === '404' || err.message.includes('stream not found')) {
      console.log(`ℹ️ Stream "${STREAM}" not found. Creating...`);
await jsm.streams.add({
  name: STREAM,
  subjects: [SUBJECT],
  retention: 'limits',
  max_msgs: 100000,
  max_bytes: 100 * 1024 * 1024, // ✅ 100 MB
  storage: 'file',
  num_replicas: 1
});
      console.log(`✅ Stream "${STREAM}" created.`);
    } else {
      throw err;
    }
  }
}

async function startOfferService() {
  // 🗂️ Ensure the local directory exists for caching
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  // 🔌 Connect to NATS JetStream
  const nc = await connect({ servers: NATS_SERVER });
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();
  const sc = StringCodec();
  const durableName = process.env.HOSTNAME || 'offer-cacher';

  // ✅ Ensure the stream exists (create if not)
  await ensureStreamExists(jsm);

  // 📬 Configure JetStream consumer options
  const opts = consumerOpts();
  opts.durable(durableName);
  opts.deliverTo('offer-cacher-inbox');
  opts.ackWait(10_000); // Wait time before redelivery if not acked
  opts.manualAck(); // We'll acknowledge only on success

  // 📥 Subscribe to the JetStream subject
  const sub = await js.subscribe(SUBJECT, opts);
  console.log(`📥 Subscribed to ${SUBJECT}`);

  // 🔁 Asynchronously process incoming messages
  (async () => {
    for await (const m of sub) {
      try {
        // 📦 Decode the message payload
        const payload = JSON.parse(sc.decode(m.data));

        // 🛑 Validate that payload has an ID
        if (!payload?.id) throw new Error("Missing payload.id");

        // ⏱️ Add timing metric to the payload (publish-time)
        payload['publish-time'] = Date.now();

        // 💾 Cache the offer file locally using the ID as the filename
        const filePath = path.join(DATA_DIR, payload.id.toString());
        fs.writeFileSync(filePath, JSON.stringify(payload), 'utf8');
        console.log(`✅ Cached: ${filePath}`);

        // 👍 Acknowledge successful processing
        m.ack();
      } catch (err) {
        console.error('❌ Failed to cache offer:', err);
        // Don't ack, to allow redelivery if needed
      }
    }
  })();

  // 🌐 Set up Express HTTP API
  const app = express();

  // 🔓 Enable CORS for all routes
  app.use(cors());

  // 📤 HTTP GET endpoint to retrieve cached offers
  app.get('/get-offers', (req, res) => {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'Missing id parameter' });

    const filePath = path.join(DATA_DIR, id);
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        console.error('❌ Failed to read offer:', err);
        return res.status(404).json({ error: 'Offer not found' });
      }

      try {
        // ⏱️ Add deliver-time metric
        const offer = JSON.parse(data);
        offer['deliver-time'] = Date.now();
        res.json(offer);
      } catch (parseErr) {
        console.error('❌ Invalid JSON in file:', parseErr);
        res.status(500).json({ error: 'Corrupt offer data' });
      }
    });
  });

  // 🚀 Start HTTP server
  const PORT = process.env.PORT || 3001;
  app.listen(PORT, () =>
    console.log(`🚀 HTTP API ready at http://localhost:${PORT}/get-offers?id=<id>`)
  );
}

startOfferService().catch(console.error);