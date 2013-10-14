var proxyquire = require('proxyquire').noCallThru();


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
            var expectedConnectionString = 'something or other';

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

    describe('AmqpEmitter', function () {
        describe('Emitting', function () {
            it('should emit events on the specified channel.', function (done) {
                var expectedQueueName = 'some queue';
                var expectedMessage = 'a message!';

                var fakeChannel = {
                    assertQueue: function (queueName) {
                        queueName.should.equal(expectedQueueName);
                    },
                    sendToQueue: function (queueName, buffer) {
                        queueName.should.equal(expectedQueueName);

                        buffer.should.eql(new Buffer(expectedMessage));

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

                var AmqpEmitter = events.connect('test');

                var emitter = new AmqpEmitter();

                emitter.emit(expectedQueueName, expectedMessage);
            });
        });
    });
});
