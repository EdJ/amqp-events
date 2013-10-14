var amqplib = require('amqplib');

// This is messy, as yet.
var createEventEmitter = function (openConnection) {
    return function EventEmitter() {
        this.emit = function (queueName, message) {
            // Obviously not looking to re-create the channel on each event,
            // will do for an initial stab.
            return openConnection.then(function (connection) {
                return connection.createChannel()
                .then(function (channel) {
                    channel.assertQueue(queueName);

                    channel.sendToQueue(queueName, new Buffer(message));
                });
            });
        };
    };
};

// Need a way to disconnect from amqp.
// Would normally provide a #disconnect() function, may be appropriate here.
module.exports = {
    connect: function (connectionString) {
        var connection = amqplib.connect(connectionString);

        return createEventEmitter(connection);
    }
};
