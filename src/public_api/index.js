const { Api } = require('@axosoft/gk-service');
const config = require('../../config/config');

const APP_NAME = 'kafka-tester-api';

const gkConfig = {
  bruteForceProtectionEnabled: false,
  kafka: {
    clientId: APP_NAME,
    connectionString: config.kafkaUrl,
    topics: ['kafka1', 'kafka2']
  },
  useRequestLogger: false
};

const stats = {
  message_count: 0,
  size_average: 0,
  size_total: 0,
  time_average: 0,
  time_total: 0
};

const sendMessage = (gkKafka, topic, event, size, id = 1) => {
  const dt = new Date();

  console.log(`sending msg ${id} with ${size} byte message for '${event}' envent on '${topic}' topic`);

  let i=0, j=size;
  const chars = [];
  while (i < j) {
    chars[i++] = chars[j--] = String.fromCharCode(65 + 24 * Math.random());
  }
  const msg = chars.join('');

  gkKafka.sendMessage(
    APP_NAME,
    topic,
    event,
    {
      id: id,
      message: msg
    }
  );

  stats.message_count++;

  const duration = new Date() - dt;
  stats.time_total += duration;
  stats.time_average = stats.time_total / stats.message_count;
  stats.size_total += size;
  stats.size_average = stats.size_total / stats.message_count;

  console.log(`sent message in ${duration}`);
};

const foreverSend = (gkKafka, topic, event, size, id, delay) => {
  sendMessage(gkKafka, topic, event, size, id);
  setTimeout(() => foreverSend(gkKafka, topic, event, size, id + 1, delay), delay);
}

const setupFn = (app, gkKafka, mongoose) => {
  app.post('/api/kafka', (req, res) => {
    // send a kafka message
    const topic = req.query.topic || 'kafka1';
    const event = req.query.event || `${topic}-event1`;
    const size = Math.min(parseInt(req.query.size) || 100, 10000);

    sendMessage(gkKafka, topic, event, size, new Date().getTime());

    res.sendStatus(200);
  });

  app.post('/api/kafka/forever', (req, res) => {
    const topic = req.query.topic || 'kafka1';
    const event = req.query.event || `${topic}-event1`;
    const size = Math.min(parseInt(req.query.size) || 100, 10000);
    const delay = Math.min(parseInt(req.query.delay) || 10, 60000);

    foreverSend(gkKafka, topic, event, size, 1, delay);
  });

  app.get('/api/kafka', (req, res) => {
    // get kafka stats
    res.status(200)
      .set('Content-Type', 'application/json')
      .send(stats);
  });
};

Api(APP_NAME, 5432, setupFn, gkConfig)
  .then(() => {
    console.log('api ready');
  })
  .catch((e) => {
    console.error(e);
  });