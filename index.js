const amqp = require('amqplib/callback_api');
const amqpCredentials = require('amqplib').credentials;
const proto = require('protobufjs');

const fs = require('fs');

const settingsPath = 'res/settings.json';
const candidateProtoPath = 'res/candidate.proto';

const settingsString = fs.readFileSync(settingsPath, 'utf-8');
const settings = JSON.parse(settingsString);
const candidatesQueueName = settings.queues.candidates;

let CandidateType = null;
let CandidateActionEnum = null;
proto.load(candidateProtoPath, (error, root) => {
    CandidateType = root.lookupType('chatbot.Candidate');
    CandidateActionEnum = root.lookupEnum('chatbot.Action');
});

const connectionOptions = {credentials: amqpCredentials.plain(settings.username, settings.password)};
amqp.connect(`amqp://${settings.host}:${settings.port}/${settings.vHost}`, connectionOptions, (connectionError, connection) => {

    if (connectionError) {
        console.error('Unable to connect to Chatbot MQ server', connectionError);
    } else {
        connection.createChannel((channelError, channel) => {
            if (channelError) {
                console.error('Unable to create channel', channelError);
            } else {
                channel.assertQueue(candidatesQueueName, {durable: true});

                console.log(`Listening to candidate events on queue '${candidatesQueueName}'`);

                channel.consume(candidatesQueueName, message => {
                    console.log('Candidate event received');

                    if (CandidateType && CandidateActionEnum) {
                        try {
                            const candidate = CandidateType.decode(message.content);

                            console.log('Candidate action', CandidateActionEnum.valuesById[candidate.action]);
                            console.log('Candidate data', candidate);

                            return channel.ack(message);
                        } catch (e) {
                            console.log('Cannot decode candidate from event', e);
                        }
                    }

                    return channel.nack(message);
                }, {noAck: false});
            }
        });
    }
});
