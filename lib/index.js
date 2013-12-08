var amqplib = require('amqplib');

// This is messy, as yet.
var createEventEmitter = function (openConnection) {
    return function EventEmitter() {
        this.emit = function (event, message) {
            // Obviously not looking to re-create the channel on each event,
            // will do for an initial stab.
            return openConnection.then(function (connection) {
                return connection.createChannel()
                    .then(function (channel) {
                        channel.assertExchange(event, 'fanout', {
                            durable: false
                        });

                        var jsonMessage = JSON.stringify(message);

                        channel.publish(event, '', new Buffer(jsonMessage));
                    });
            });
        };

        this.on = function (event, callback) {
            // Nesting!
            return openConnection.then(function (connection) {
                return connection.createChannel()
                    .then(function (channel) {
                        return channel.assertExchange(event, 'fanout', {
                            durable: false
                        }).then(function () {
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
                                    return channel.consume(queueName, function (message) {
                                        var content = message.content.toString();
                                        content = JSON.parse(content);

                                        callback(content);

                                        channel.ack(message);
                                    });
                                });
                            });
                        });
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
    close: function () {
        if (this._connectionPromise) {
            this._connectionPromise.then(function (connection) {
                connection.close();
            });
        }
    }
};
