import { kafka } from './client.js';
const group = process.argv[2];

async function init(){
    const consumer=kafka.consumer({ groupId: group });

    await consumer.connect();

    await consumer.subscribe({topic: 'rider-updates', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
                group
            });
        },
    })


}

init();