const { connect, StringCodec, consumerOpts } = require('nats');
const express = require('express');
const fs = require('fs');
const path = require('path');
const cors = require('cors');

const DATA_DIR = '/tmp/data';
const NATS_SERVER = 'localhost:4222';
const SUBJECT = 'publish-offers.*';



async function startOfferService() {
  // Ensure data dir exists
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  // Set up NATS JetStream
  const nc = await connect({ servers: NATS_SERVER });
  const js = nc.jetstream();
  const sc = StringCodec();
  const durableName = process.env.HOSTNAME || 'offer-cacher';

  const opts = consumerOpts();
  opts.durable(durableName);
  opts.deliverTo('offer-cacher-inbox');
  opts.ackWait(10_000);
  opts.manualAck();

  const sub = await js.subscribe(SUBJECT, opts);
  console.log(`📥 Subscribed to ${SUBJECT}`);

  (async () => {
    for await (const m of sub) {
      try {
const payload = JSON.parse(sc.decode(m.data));
if (!payload.id) throw new Error("Missing payload.id");

payload['publish-time'] = Date.now();

const filePath = path.join(DATA_DIR, payload.id.toString());
fs.writeFileSync(filePath, JSON.stringify(payload), 'utf8');
console.log(`✅ Cached: ${filePath}`);

        m.ack();
      } catch (err) {
        console.error('❌ Failed to cache offer:', err);
      }
    }
  })();

  // Set up Express HTTP endpoint
  const app = express();
  
  // Enable CORS for all routes
app.use(cors());

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
        const offer = JSON.parse(data);
        offer['deliver-time'] = Date.now();
        res.json(offer);
      } catch (parseErr) {
        console.error('❌ Invalid JSON in file:', parseErr);
        res.status(500).json({ error: 'Corrupt offer data' });
      }
    });
  });

  const PORT = process.env.PORT || 3001;
  app.listen(PORT, () => console.log(`🚀 HTTP API ready at http://localhost:${PORT}/get-offers?id=<id>`));
}

startOfferService().catch(console.error);
