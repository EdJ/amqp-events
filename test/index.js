var proxyquire = require('proxyquire').noCallThru();
var when = require('when');

var fulfilledPromise = function (result) {
    return when.promise(function (resolve) {
        resolve(result);
    });
};

describe('amqp-events', function () {
    var getEvents;

    beforeEach(function () {
        getEvents = function (fakes) {
            fakes = fakes || {};

            return proxyquire('../lib', {
                'amqplib': fakes.amqplib || {}
            });
        };
    });

    describe('#connect(options)', function () {
        it('should open an underlying amqplib connection.', function (done) {
            var expectedConnectionString = 'amqp://something or other';

            var fakeAmqplib = {
                connect: function (connectionString) {
                    connectionString.should.equal(expectedConnectionString);

                    done();
                }
            };

            var events = getEvents({
                amqplib: fakeAmqplib
            });

            events.connect(expectedConnectionString);
        });
    });

    describe('#disconnect()', function () {
        it('should call #close on the underlying connection.', function (done) {
            var connection = {
                createChannel: function () {
                    return fulfilledPromise({});
                },
                close: function () {
                    done();
                }
            };

            var fakeAmqplib = {
                connect: function () {
                    return fulfilledPromise(connection);
                }
            };

            var events = getEvents({
                amqplib: fakeAmqplib
            });

            events.connect('amqp://somewhere');

            events.disconnect();
        });
    });

    describe('AmqpEmitter', function () {
        describe('Emitting', function () {
            it('should emit events on the specified exchange.', function (done) {
                var expectedQueueName = 'some queue';
                var expectedMessage = {
                    someData: 'test'
                };

                var fakeChannel = {
                    assertExchange: function (queueName, type, options) {
                        queueName.should.equal(expectedQueueName);
                        type.should.equal('fanout');
                        options.should.eql({
                            durable: false
                        });

                        return fulfilledPromise();
                    },
                    publish: function (queueName, routingKey, buffer) {
                        queueName.should.equal(expectedQueueName);
                        routingKey.should.equal('');

                        var stringified = JSON.stringify(expectedMessage);

                        buffer.should.eql(new Buffer(stringified));

                        done();
                    }
                };

                var fakeConnection = {
                    createChannel: function () {
                        return {
                            then: function (callback) {
                                callback(fakeChannel);
                            }
                        };
                    }
                };

                var fakeAmqplib = {
                    connect: function () {
                        return {
                            then: function (callback) {
                                callback(fakeConnection);
                            }
                        };
                    }
                };

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://somewhere');

                var emitter = new AmqpEmitter();

                emitter.emit(expectedQueueName, expectedMessage);
            });
        });

        describe('Receiving', function () {
            it('should bind a queue to the relevant exchange and a handler to that queue.', function (done) {
                var expectedEventName = 'some queue';
                var expectedResult = 'a message!';

                var messageToSend = {
                    content: new Buffer(JSON.stringify(expectedResult))
                };

                var assertedQueue = {
                    queue: 'some generated value'
                };

                var performedCallback = false;

                var fakeChannel = {
                    assertQueue: function (queueName, options) {
                        queueName.should.equal('');

                        options.exclusive.should.equal(false);

                        // Queues bound with \'\' should have autoDelete = true,
                        // or we end up with a bunch of inaccessible queues on the
                        // server.
                        options.autoDelete.should.equal(true);

                        return fulfilledPromise(assertedQueue);
                    },
                    bindQueue: function (queueName, exchangeName, routingKey) {
                        routingKey.should.equal('');
                        queueName.should.equal(assertedQueue.queue);
                        exchangeName.should.equal(expectedEventName);

                        return fulfilledPromise(null);
                    },
                    assertExchange: function (queueName, type, options) {
                        queueName.should.equal(expectedEventName);
                        type.should.equal('fanout');
                        options.should.eql({
                            durable: false
                        });

                        return fulfilledPromise(null);
                    },
                    consume: function (queueName, callback) {
                        // This is generated by the AMQP server.
                        queueName.should.equal(assertedQueue.queue);

                        callback(messageToSend);
                    },
                    ack: function (message) {
                        message.should.equal(messageToSend);

                        performedCallback.should.equal(true);

                        done();
                    }
                };

                var fakeConnection = {
                    createChannel: function () {
                        return {
                            then: function (callback) {
                                callback(fakeChannel);
                            }
                        };
                    }
                };

                var fakeAmqplib = {
                    connect: function () {
                        return {
                            then: function (callback) {
                                callback(fakeConnection);
                            }
                        };
                    }
                };

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://somewhere');

                var emitter = new AmqpEmitter();

                var callback = function (result) {
                    result.should.equal(expectedResult);

                    performedCallback = true;
                };

                emitter.on(expectedEventName, callback);
            });
        });
    });
});
