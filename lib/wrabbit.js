var amqp = require('amqplib')
  , EventEmitter = require('events').EventEmitter
  , Logger = require('./logger')
  , Queue = require('./queue');

function WRabbit(url, prefetch, logger) {
  if (!url) throw new Error('url required for jackrabbit connection');

  Logger.init(logger);
  EventEmitter.call(this);

  this.connection = null;
  this.channel = null;
  this.prefetch = prefetch || 1;
  this.queues = {};

  amqp
    .connect(url)
    .then(this.createChannel.bind(this))
    .then(this.onChannel.bind(this))
    .catch(this.onConnectionErr.bind(this));
}

module.exports = function createJackRabbit(url, prefetch, logger) {
  return new WRabbit(url, prefetch, logger);
};

WRabbit.prototype = Object.create(EventEmitter.prototype);

WRabbit.prototype.createChannel = function(connection) {
  this.connection = connection;
  this.connection.once('close', this.onClose.bind(this));
  this.connection.on('error', this.onConnectionErr.bind(this));
  return connection.createConfirmChannel();
};

WRabbit.prototype.onChannel = function(channel) {
  this.channel = channel;

  // Create a reply queue
  this.channel.replyHandlers = {};
  this.channel.assertQueue('', { exclusive: true })
    .then(function(replyTo) {
      this.channel.replyName = replyTo.queue;
      this.channel.consume(this.replyName, this.onReply.bind(this), { noAck: true });
      this.emit('connected');
    }.bind(this));

  this.channel.on('close', this.onChannelClose.bind(this));
  this.channel.prefetch(this.prefetch);
};

WRabbit.prototype.onReply = function(msg) {
  var id = msg.properties.correlationId;
  var replyHandler = this.channel.replyHandlers[id];
  if (!replyHandler) return;
  delete this.channel.replyHandlers[id];

  var body = msg.content.toString();
  var obj = JSON.parse(body);
  replyHandler(null, obj);
};

WRabbit.prototype.close = function() {
  this.connection.close();
};

WRabbit.prototype.onClose = function() {
  this.emit('disconnected');
};

WRabbit.prototype.onConnectionErr = function(err) {
  this.emit('disconnected', err);
};

WRabbit.prototype.onChannelClose = function() {
  this.emit('disconnected');
};

WRabbit.prototype.queue = function(name, options) {
  return new Queue(this.channel, name, options);
};

WRabbit.prototype.create = function(name, options, done) {
  if (!done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    else {
      done = function() {};
    }
  }
  var queue = new Queue(this.channel, name, options);
  queue
    .once('ready', function onQueueReady(info) {
      this.queues[name] = queue;
      done(null, queue, info);
    }.bind(this))
    .once('error', function onQueueErr(err) {
      done(err);
    });
};

WRabbit.prototype.destroy = function(name, done) {
  this.channel
    .deleteQueue(name)
    .then(onSuccess)
    .catch(onFail);

  function onSuccess() { done(null, true); }
  function onFail(err) { done(err); }
};

WRabbit.prototype.purge = function(name, done) {
  this.channel
    .purgeQueue(name)
    .then(onSuccess)
    .catch(onFail);

  function onSuccess(response) { done(null, response.messageCount); }
  function onFail(err) { done(err); }
};

WRabbit.prototype.publish = function(name, obj, headers, cb, replyHandler) {
  if(typeof headers  === 'function'){
    replyHandler = cb;
    cb = headers;
    headers = {};
  }
  if (!headers) {
    headers = {};
  }
  this.queues[name].publish(obj, headers, cb, replyHandler);
};

WRabbit.prototype.handle = function(name, handler) {
  this.queues[name].subscribe(handler);
};

WRabbit.prototype.ignore = function(name) {
  this.queues[name].unsubscribe();
};
