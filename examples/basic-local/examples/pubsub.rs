use std::{thread::sleep, time::Duration};

use bonsaidb::{
    core::pubsub::{PubSub, Subscriber},
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};

fn main() -> Result<(), bonsaidb::local::Error> {
    // This example is using a database with no collections, because PubSub is a
    // system independent of the data stored in the database.
    let db = Database::open::<()>(StorageConfiguration::new("pubsub.bonsaidb"))?;

    let subscriber = db.create_subscriber()?;
    // Subscribe for messages sent to the topic "pong"
    subscriber.subscribe_to(&"pong")?;

    // Launch a thread that sends out "ping" messages.
    let thread_db = db.clone();
    std::thread::spawn(move || pinger(thread_db));
    // Launch a thread that receives "ping" messages and sends "pong" responses.
    std::thread::spawn(move || ponger(db));

    // Loop until a we receive a message letting us know when the ponger() has
    // no pings remaining.
    loop {
        let message = subscriber.receiver().receive()?;
        let pings_remaining = message.payload::<usize>()?;
        println!(
            "<-- Received {}, pings remaining: {}",
            message.topic::<String>()?,
            pings_remaining
        );
        if pings_remaining == 0 {
            break;
        }
    }

    println!("Received all pongs.");

    Ok(())
}

fn pinger<P: PubSub>(pubsub: P) -> Result<(), bonsaidb::local::Error> {
    let mut ping_count = 0u32;
    loop {
        ping_count += 1;
        println!("-> Sending ping {}", ping_count);
        pubsub.publish(&"ping", &ping_count)?;
        sleep(Duration::from_millis(250));
    }
}

fn ponger<P: PubSub>(pubsub: P) -> Result<(), bonsaidb::local::Error> {
    const NUMBER_OF_PONGS: usize = 5;
    let subscriber = pubsub.create_subscriber()?;
    subscriber.subscribe_to(&"ping")?;
    let mut pings_remaining = NUMBER_OF_PONGS;

    println!(
        "Ponger started, waiting to respond to {} pings",
        pings_remaining
    );

    while pings_remaining > 0 {
        let message = subscriber.receiver().receive()?;
        println!(
            "<- Received {}, id {}",
            message.topic::<&str>().unwrap(),
            message.payload::<u32>().unwrap()
        );
        pings_remaining -= 1;
        pubsub.publish(&"pong", &pings_remaining)?;
    }

    println!("Ponger finished.");

    Ok(())
}

#[test]
fn runs() {
    main().unwrap()
}
