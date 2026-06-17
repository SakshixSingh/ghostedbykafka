import { kafka } from './client.js';

async function init(){
    const producer=kafka.producer();

    console.log('Connecting Producer...');
    await producer.connect();
    console.log('Producer Connected...');

    await producer.send({
        topic: 'rider-updates',
        messages: [
            {
                partition: 0,
                key : 'location-update',
                value : JSON.stringify({
                    name : 'Sakshi Singh hahaha',
                    loc : 'south'
                })
            }
        ],
    })

    producer.disconnect();
}

init();