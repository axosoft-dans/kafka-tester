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

const setupFn = (app, gkKafka, mongoose) => {
  console.log('setup');

  app.post('/api/kafka', (req, res) => {
    // send a kafka message
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
  });

  app.get('/api/kafka', (req, res) => {
    // get kafka stats
  });
};

Api(APP_NAME, 5432, setupFn, gkConfig)
  .then(() => {
    console.log('api ready');
  });