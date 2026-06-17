import { kafka } from './client.js';


async function init(){
    const consumer=kafka.consumer({ groupId: 'group-01' });

    await consumer.connect();

    await consumer.subscribe({topic: 'rider-updates', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    })


}

init();