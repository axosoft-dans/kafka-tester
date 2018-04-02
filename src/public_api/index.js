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
  time_average: 0,
  time_total: 0
}

const setupFn = (app, gkKafka, mongoose) => {
  app.post('/api/kafka', (req, res) => {
    // send a kafka message
    const dt = new Date();

    const topic = req.query.topic || 'kafka1';
    const event = req.query.event || 'kafka1-event1';
    const size = Math.min(parseInt(req.query.size) || 100, 10000);

    console.log(`sending '${size}' byte message for '${event}' envent on '${topic}' topic`);

    let i=0, j=size;
    const chars = [];
    while (i < j) {
      chars[i++] = chars[j--] = String.fromCharCode(65 + 24 * Math.random());
    }
    const msg = chars.join('');
    console.log('msg', msg);

    gkKafka.sendMessage(
      APP_NAME,
      topic,
      event,
      {
        message: msg
      }
    );

    const duration = new Date() - dt;
    stats.time_total += duration;
    stats.message_count++;
    stats.time_average = stats.time_total / stats.message_count;

    console.log(`sent message in ${duration}`);
    res.sendStatus(200);
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