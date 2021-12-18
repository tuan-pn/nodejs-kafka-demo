const {Kafka} = require('kafkajs')

const msg = process.argv[2];
if (!msg) {
  throw new Error('Missing message')
}
run();

async function run() {
  try {
    // connect with the broker
    const kafka = new Kafka({
      clientId: 'myapp',
      brokers: ['localhost:9092']
    });

    const producer = kafka.producer();
    console.log("Connecting producer ...")
    await producer.connect();
    console.log("Connected producer!")

    // A - M: 0
    // N - Z: 1
    const partition = msg[0] < "N" ? 0 : 1;
    const result = await producer.send({
      topic: "Users",
      messages: [{
        value: msg,
        partition: partition
      }]
    })
    console.log(`Send successfully! ${JSON.stringify(result)}`)
    await producer.disconnect();

  
  } catch (e) {
    console.error(e.stack);
  } finally {
    process.exit(0);
  }
}