var amqplib = require('amqplib');
var when = require('when');

var fulfilledPromise = function (result) {
    return when.promise(function (resolve) {
        resolve(result);
    });
};

// This is messy, as yet.
var createEventEmitter = function (openConnection) {
    return function EventEmitter() {
        this.exchanges = {};

        var self = this;

        this._sharedChannel = openConnection.then(function (connection) {
            return connection.createChannel();
        });

        this._ensureExchange = function _ensureExchange(exchange) {
            if (this.exchanges[exchange]) {
                return fulfilledPromise(exchange);
            }

            return this._sharedChannel.then(function (channel) {
                return channel.assertExchange(exchange, 'fanout', {
                    durable: false
                });
            }).then(function () {
                self.exchanges[exchange] = true;

                return exchange;
            });
        };

        this.emit = function emit(event, message) {
            return this._ensureExchange(event).then(function () {
                return self._sharedChannel;
            }).then(function (channel) {
                var jsonMessage = JSON.stringify(message);

                return channel.publish(event, '', new Buffer(jsonMessage));
            });
        };

        this._knownQueues = {};

        this._ensureQueue = function _ensureQueue(event) {
            var possibleQueue = this._knownQueues[event];

            if (possibleQueue) {
                return fulfilledPromise(possibleQueue);
            }

            return this._sharedChannel.then(function (channel) {
                // Huge assumption...
                // These options specify queues that do not survive all
                // their consumers disconnecting, but that are shared between
                // multiple consumers (which isn't possible as the queue names
                // are generated on the AMQP server).
                var options = {
                    exclusive: false,
                    autoDelete: true
                };

                return channel.assertQueue('', options).then(function (queue) {
                    var queueName = queue.queue;

                    return channel.bindQueue(queueName, event, '').then(function () {
                        self._knownQueues[event] = queueName;
                        return queueName;
                    });
                });
            });
        };

        this._decodeMessage = function _decodeMessage(message) {
            var content = message.content.toString();
            return JSON.parse(content);
        };

        this._handleMessage = function _handleMessage(channel, callback) {
            if (callback.length == 2) {
                return function _handleMessage_callback(message) {
                    var content = self._decodeMessage(message);

                    return callback(content, function () {
                        channel.ack(message);
                    });
                };
            }

            return function _handleMessage_curry(message) {
                var content = self._decodeMessage(message);

                var result = callback(content);

                if (result && result.then) {
                    return result.then(function () {
                        channel.ack(message);
                    });
                }

                channel.ack(message);
            };
        };

        this.on = function on(event, callback) {
            return this._ensureExchange(event)
                .then(self._ensureQueue.bind(this))
                .then(function (queueName) {
                    return self._sharedChannel.then(function (channel) {
                        return channel.consume(queueName, self._handleMessage(channel, callback));
                    });
                });
        };
    };
};

// Need a way to disconnect from amqp.
// Would normally provide a #disconnect() function, may be appropriate here.
module.exports = {
    connect: function (connectionString) {
        this._connectionPromise = amqplib.connect(connectionString);

        return createEventEmitter(this._connectionPromise);
    },
    disconnect: function () {
        if (this._connectionPromise) {
            this._connectionPromise.then(function (connection) {
                connection.close();
            });
        }
    }
};
