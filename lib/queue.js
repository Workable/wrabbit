var EventEmitter = require('events').EventEmitter
  , uuid = require('uuid')
  , _ = require('lodash')
  , Logger = require('./logger');

function Queue(channel, name, options) {
  EventEmitter.call(this);
  options = _.extend({}, { durable: true, noAck: false, messageTtl: 1000 }, options);

  this.handler = function() {};
  this.name = name;
  this.replyName = null;
  this.channel = channel;
  this.durable = options.durable;
  this.priority = options.priority;
  this.exclusive = options.exclusive;
  this.noAck = options.noAck;
  this.tag = null;

  var queueOptions = { durable: this.durable};

  if (this.priority) {
    queueOptions = _.extend({arguments: {'x-max-priority': this.priority}}, queueOptions);
  }

  if (this.exclusive) {
    queueOptions = _.extend({exclusive: this.exclusive}, queueOptions);
  }

  this.channel
    .assertQueue(name, queueOptions)
    .then(function createReplyQueue(info) {
      this.emit('ready', info);
    }.bind(this))
    .catch(function(error) {
      this.emit('error', error)
    }.bind(this));
}

Queue.prototype = Object.create(EventEmitter.prototype);

module.exports = Queue;

Queue.prototype.subscribe = function(handler) {
  this.handler = handler;
  var tag = this.channel
    .consume(this.name, this.onMessage.bind(this), { noAck: this.noAck })
    .then(saveTag.bind(this));

  // TODO: is there a race condition here if this isn't called before unsubscribe?
  function saveTag(obj) {
    this.tag = obj.consumerTag;
  }
};

Queue.prototype.unsubscribe = function() {
  this.channel.cancel(this.tag);
  this.handler = null;
  this.tag = null;
};

Queue.prototype.onMessage = function(msg) {
  try {
    if (!msg) return;
    var body = msg.content.toString();
    var obj = JSON.parse(body);
    var hasReply = msg.properties.replyTo;

    if (hasReply && !this.noAck) this.channel.ack(msg);
    this.handler(obj, function(reply) {
      if (hasReply) {
        var replyBuffer = new Buffer(JSON.stringify(reply || ''));
        this.channel.sendToQueue(msg.properties.replyTo, replyBuffer, {
          correlationId: msg.properties.correlationId
        });
      }
      else if (!this.noAck) {
        this.channel.ack(msg);
      }
    }.bind(this));
  } catch (error){
    error.message = "QUEUE_PARSING_ERROR: Ingoring msg: " + body + " because of error: " + error.message;
    Logger.getLogger().error(error);
    this.channel.ack(msg);
  }

};

Queue.prototype.publish = function(obj, headers, cb, replyHandler) {
  var msg = JSON.stringify(obj);
  var id = uuid.v4();
  if (replyHandler) {
    this.channel.replyHandlers[id] = replyHandler;
  }
  var defaultHeaders = {
    persistent: !replyHandler,
    correlationId: id,
    replyTo: replyHandler ? this.channel.replyName : undefined
  };
  headers = _.extend(defaultHeaders, headers);
  this.channel.sendToQueue(this.name, new Buffer(msg), headers, cb);
};
