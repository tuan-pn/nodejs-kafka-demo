const {Kafka} = require('kafkajs')

run();

async function run() {
  try {
    // connect with the broker
    const kafka = new Kafka({
      clientId: 'myapp',
      brokers: ['localhost:9092']
    });

    const consumer = kafka.consumer({groupId: "test"});
    console.log("Connecting consumer...")
    await consumer.connect();
    console.log("Connected consumer!")

    await consumer.subscribe({
      topic: "Users",
      fromBeginning: true,
    })
    await consumer.run({
      eachMessage: async result => {
        console.log(`Receive msg:: ${result.message.value} on partition ${result.partition}`)
      }
    })
  } catch (e) {
    console.error(e.stack);
  }
}