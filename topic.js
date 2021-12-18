const {Kafka} = require('kafkajs')

run();

async function run() {
  try {
    // connect with the broker
    const kafka = new Kafka({
      clientId: 'myapp',
      brokers: ['localhost:9092']
    });

    const admin = kafka.admin();
    console.log("Connecting ...")
    await admin.connect();
    console.log("Connected!")

    // A-M, N-Z
    await admin.createTopics({topics: [{
      topic: "Users",
      numPartitions: 2
    }]})
    console.log("Create topic successfully")
  } catch (e) {
    console.error(e.stack);
  } finally {
    process.exit(0);
  }
}