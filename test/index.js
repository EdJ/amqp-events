var proxyquire = require('proxyquire').noCallThru();
var when = require('when');

var fulfillPromise = function (result) {
    return when.promise(function (resolve) {
        resolve(result);
    });
};

var fulfillOnCall = function fulfillOnCall(result) {
    return function fulfilled() {
        return fulfillPromise(result);
    };
};

var getFakeAmqplib = function (fakeChannel) {
    var fakeConnection = {
        createChannel: function () {
            return fulfillPromise(fakeChannel);
        }
    };

    return {
        connect: function () {
            return fulfillPromise(fakeConnection);
        }
    };
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
                createChannel: fulfillPromise,
                close: function () {
                    done();
                }
            };

            var fakeAmqplib = {
                connect: fulfillOnCall(connection)
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
                var expectedEventName = 'some queue';
                var expectedMessage = {
                    someData: 'test'
                };

                var fakeChannel = {
                    assertExchange: function (exchangeName, type, options) {
                        exchangeName.should.equal(expectedEventName);
                        type.should.equal('fanout');
                        options.should.eql({
                            durable: false
                        });

                        return fulfillPromise();
                    },
                    publish: function (exchangeName, routingKey, buffer) {
                        exchangeName.should.equal(expectedEventName);
                        routingKey.should.equal('');

                        var stringified = JSON.stringify(expectedMessage);

                        buffer.should.eql(new Buffer(stringified));

                        done();
                    }
                };

                var fakeAmqplib = getFakeAmqplib(fakeChannel);

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://somewhere');

                var emitter = new AmqpEmitter();

                emitter.emit(expectedEventName, expectedMessage);
            });

            it('should not open two channels.', function (done) {
                var fakeChannel = {
                    assertExchange: fulfillPromise,
                    publish: fulfillPromise
                };

                var fakeConnection = {
                    createChannel: function () {
                        this.createChannel = function () {
                            done('Should not open another channel.');
                        };

                        return fulfillPromise(fakeChannel);
                    }
                };

                var fakeAmqplib = {
                    connect: function () {
                        return fulfillPromise(fakeConnection);
                    }
                };

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://sometestserver');

                var emitter = new AmqpEmitter();

                var expectedEvent = 'an event';

                emitter.emit(expectedEvent, 'a message').then(function () {
                    return emitter.emit(expectedEvent, 'some other message');
                }).then(function () {
                    done();
                });
            });

            it('should not re-assert a known exchange.', function (done) {
                var fakeChannel = {
                    assertExchange: function () {
                        this.assertExchange = function () {
                            done('Should not re-assert the exchange.');
                        };

                        return fulfillPromise();
                    },
                    publish: fulfillPromise
                };

                var events = getEvents({
                    amqplib: getFakeAmqplib(fakeChannel)
                });

                var AmqpEmitter = events.connect('amqp://sometestserver');

                var emitter = new AmqpEmitter();

                var expectedEvent = 'some queue';

                emitter.emit(expectedEvent, 'a message').then(function () {
                    return emitter.emit(expectedEvent, 'some other message');
                }).then(function () {
                    done();
                });
            });
        });

        describe('Receiving', function () {
            it('should bind a queue to the relevant exchange and a handler to that queue.', function (done) {
                var expectedEventName = 'some event';
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
                        // Queue names should be generated by the emitter.
                        queueName.should.not.equal('');

                        // Shared queues are essential to support multiple workers.
                        options.exclusive.should.equal(false);

                        // Queues bound with \'\' should have autoDelete = true,
                        // or we end up with a bunch of inaccessible queues on the
                        // server.
                        options.autoDelete.should.equal(true);

                        return fulfillPromise(assertedQueue);
                    },
                    bindQueue: function (queueName, exchangeName, routingKey) {
                        routingKey.should.equal('');
                        queueName.should.equal(assertedQueue.queue);
                        exchangeName.should.equal(expectedEventName);

                        return fulfillPromise();
                    },
                    assertExchange: function (queueName, type, options) {
                        queueName.should.equal(expectedEventName);
                        type.should.equal('fanout');
                        options.should.eql({
                            durable: false
                        });

                        return fulfillPromise();
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

                var fakeAmqplib = getFakeAmqplib(fakeChannel);

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

            it('should wait for a returned promise to ack.', function (done) {
                var expectedResult = 'a message!';

                var messageToSend = {
                    content: new Buffer(JSON.stringify(expectedResult))
                };

                var performedCallback = false;

                var fakeChannel = {
                    assertQueue: function () {
                        return fulfillPromise({
                            queue: 'some generated value'
                        });
                    },
                    bindQueue: fulfillPromise,
                    assertExchange: fulfillPromise,
                    consume: function (queueName, callback) {
                        callback(messageToSend);
                    },
                    ack: function (message) {
                        message.should.equal(messageToSend);

                        performedCallback.should.equal(true);

                        done();
                    }
                };

                var fakeAmqplib = getFakeAmqplib(fakeChannel);

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://somewhere');

                var emitter = new AmqpEmitter();

                var callback = function (result) {
                    result.should.equal(expectedResult);

                    return when.promise(function (resolve) {
                        setTimeout(function () {
                            performedCallback = true;

                            resolve();
                        }, 1);
                    });
                };

                emitter.on('some event', callback);
            });

            it('should provide a callback for acks.', function (done) {
                var expectedResult = 'a message!';

                var messageToSend = {
                    content: new Buffer(JSON.stringify(expectedResult))
                };

                var performedCallback = false;

                var fakeChannel = {
                    assertQueue: function () {
                        return fulfillPromise({
                            queue: 'some generated value'
                        });
                    },
                    bindQueue: fulfillPromise,
                    assertExchange: fulfillPromise,
                    consume: function (queueName, callback) {
                        callback(messageToSend);
                    },
                    ack: function (message) {
                        message.should.equal(messageToSend);

                        performedCallback.should.equal(true);

                        done();
                    }
                };

                var fakeAmqplib = getFakeAmqplib(fakeChannel);

                var events = getEvents({
                    amqplib: fakeAmqplib
                });

                var AmqpEmitter = events.connect('amqp://somewhere');

                var emitter = new AmqpEmitter();

                var callback = function (result, callback) {
                    result.should.equal(expectedResult);

                    setTimeout(function () {
                        performedCallback = true;

                        callback();
                    }, 1);
                };

                emitter.on('some event', callback);
            });

            it('should not re-assert known exchanges, or re-bind known queues.', function (done) {
                var fakeChannel = {
                    assertQueue: function () {
                        this.assertExchange = function () {
                            done('Should not re-assert the queue.');
                        };

                        return fulfillPromise();
                    },
                    assertExchange: function () {
                        this.assertExchange = function () {
                            done('Should not re-assert the exchange.');
                        };

                        return fulfillPromise();
                    },
                    bindQueue: function () {
                        this.bindQueue = function () {
                            done('Should not re-bind the queue.');
                        };

                        return fulfillPromise();
                    },
                    consume: function () {}
                };

                var events = getEvents({
                    amqplib: getFakeAmqplib(fakeChannel)
                });

                var AmqpEmitter = events.connect('amqp://some test server');

                var emitter = new AmqpEmitter();

                var expectedQueueName = 'some queue';

                emitter.on(expectedQueueName, function () {});

                setTimeout(function () {
                    emitter.on(expectedQueueName, function () {});
                }, 50);

                setTimeout(function () {
                    done();
                }, 100);
            });

            it('should share a queue name for two identical listening functions.', function (done) {
                var fakeChannel = {
                    assertQueue: function (queueName) {
                        this.assertQueue = function (secondQueueName) {
                            secondQueueName.should.equal(queueName);

                            done();
                        };

                        return fulfillPromise();
                    },
                    assertExchange: fulfillPromise,
                    bindQueue: fulfillPromise,
                    consume: function () {}
                };

                var events = getEvents({
                    amqplib: getFakeAmqplib(fakeChannel)
                });

                var AmqpEmitter = events.connect('amqp://some test server');

                var emitter = new AmqpEmitter();

                var expectedQueueName = 'some queue';

                var handlerFunction = function () {
                    // We would do stuff here.
                };

                emitter.on(expectedQueueName, handlerFunction);

                emitter.on(expectedQueueName, handlerFunction);
            });

            it('should generate separate queue names for two different listening functions.', function (done) {
                var fakeChannel = {
                    assertQueue: function (queueName) {
                        this.assertQueue = function (secondQueueName) {
                            secondQueueName.should.not.equal(queueName);

                            done();
                        };

                        return fulfillPromise();
                    },
                    assertExchange: fulfillPromise,
                    bindQueue: fulfillPromise,
                    consume: function () {}
                };

                var events = getEvents({
                    amqplib: getFakeAmqplib(fakeChannel)
                });

                var AmqpEmitter = events.connect('amqp://some test server');

                var emitter = new AmqpEmitter();

                var expectedQueueName = 'some queue';

                emitter.on(expectedQueueName, function () {
                    // Something happens here.
                });

                emitter.on(expectedQueueName, function () {
                    // Something different happens here.
                });
            });
        });
    });
});
