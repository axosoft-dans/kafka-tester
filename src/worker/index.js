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

const stats = {
  k1_message_count: 0,
  k1_delay_average: 0,
  k1_delay_total: 0,
  k2_message_count: 0,
  k2_delay_average: 0,
  k2_delay_total: 0
}

const eventHandler = (message) => {
  const dt = new Date();
  const delay = dt - new Date(message.timeStamp);

  //console.log('message', message);
  console.log('delay', delay);

  if (message.event === 'kafka1-event1' || message.event === 'kafka1-event2') {
    // kafka1
    stats.k1_message_count++;
    stats.k1_delay_total += delay;
    stats.k1_delay_average = stats.k1_delay_total / stats.k1_message_count;

    console.log(`k1: ${stats.k1_message_count} msgs received with ${stats.k1_delay_average} avg delay`);
  }
  else {
    // kafka2
    stats.k2_message_count++;
    stats.k2_delay_total += delay;
    stats.k2_delay_average = stats.k2_delay_total / stats.k2_message_count;

    console.log(`k2: ${stats.k2_message_count} msgs received with ${stats.k2_delay_average} avg delay`);
  }
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
