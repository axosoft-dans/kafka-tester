const { Worker } = require('@axosoft/gk-service');
const config = require('../../config/config');

const APP_NAME = 'kafka-tester-worker';

const gkConfig = {
  kafka: {
    clientId: APP_NAME,
    connectionString: config.kafkaUrl,
    topics: ['kafka1', 'kafka2']
  },
  useRequestLogger: false
};

const eventHandler = (message) => {
  console.log('message', message);
};

const messageHandlers = (logger, gkKafka, mongoose) => {
  console.log('handler');

  return {
    ['kafka1']: {
      ['kafka1-event1']: eventHandler,
      ['kafka1-event2']: eventHandler
    },
    ['kafka2']: {
      ['kafka2-event1']: eventHandler,
      ['kafka2-event2']: eventHandler
    }
  }
};

Worker(APP_NAME, messageHandlers, gkConfig)
  .then(({ gkKafka }) => {
    console.log('worker ready');
  })
  .catch((e) => {
    console.error(e);
  });
