var wrabbit = require('./');
var queue = wrabbit('amqp://localhost');

queue.on('connected', function() {
  queue.create('jobs.greet', { prefetch: 5 }, onReady);

  function onReady() {
    queue.handle('jobs.greet', onJob);
    queue.publish('jobs.greet', { name: 'Hunter' });
  }

  function onJob(job, ack) {
    console.log('Hello, ' + job.name);
    ack();
    process.exit();
  }
});
