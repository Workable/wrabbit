var assert = require('chai').assert;
var jackrabbit = require('..');
var Queue = require('../lib/queue');
var util = require('./util');
var sinon = require('sinon')
    , sandbox = sinon.sandbox.create();

describe('jackrabbit', function() {

  describe('rpc', function() {

    before(function connect(done) {
      this.onMessageSpy = sandbox.spy(Queue.prototype, 'onMessage');
      this.client = jackrabbit(util.RABBIT_URL, 1);
      this.client.once('connected', done);
    });

    before(function connect(done) {
      this.server = jackrabbit(util.RABBIT_URL, 1);
      this.server.once('connected', done);
    });

    before(function createQueue(done) {
      this.name = util.NAME + '.rpc-add';
      this.client.create(this.name, done);
    });

    before(function createQueue(done) {
      this.server.create(this.name, done);
    });

    before(function createHandler() {
      this.server.handle(this.name, function add(message, reply) {
        reply(message.a + message.b);
      });
    });

    after(function cleanup(done) {
      this.client.destroy(this.name, done);
      sandbox.restore();
    });

    afterEach(function cleanupSandbox() {
      this.onMessageSpy.reset();
    });

    it('handles an rpc response', function(done) {
      this.client.publish(this.name, { a: 2, b: 3 }, function() {}, function onResponse(err, response) {
        assert.ok(!err);
        assert.equal(response, 5);
        done();
      });
    });

    it('should create exclusive queues', function(done) {
      this.exclusiveName = util.NAME + '.rpc-add-exclusive';
      this.client.create(this.exclusiveName, {exclusive: true}, done);
    });

    it('should create priority queues', function(done) {
      this.priorityName = util.NAME + '.rpc-add-priority';
      this.client.create(this.priorityName, {priority: 10}, done);
    });

    it('handles an rpc response with undefined headers', function(done) {
      this.client.publish(this.name, { a: 2, b: 3 }, undefined, function() {}, function onResponse(err, response) {
        assert.ok(!err);
        assert.equal(response, 5);
        done();
      });
    });

    it('handles an rpc response with priority header', function(done) {
      this.client.publish(this.name, { a: 2, b: 3 }, {priority: 5}, function() {}, function onResponse(err, response) {
        assert.ok(!err);
        assert.equal(response, 5);
        assert.ok(this.onMessageSpy.calledOnce);
        assert.equal(this.onMessageSpy.getCall(0).args[0].properties.priority, 5);
        done();
      }.bind(this));
    });

    it('handles an publish ack', function(done) {
      this.client.publish(this.name, { a: 2, b: 3 }, function publishAck(err, ok) {
        assert.ok(!err);
        done();
      });
    });

  });
});
