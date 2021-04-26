use std::time::Duration;

use pliantdb::local::{Configuration, Database};
use pliantdb_core::pubsub::{PubSub, Subscriber};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // This example is using a database with no collections, because PubSub is a
    // system independent of the data stored in the database.
    let db = Database::<()>::open_local("pubsub.pliantdb", &Configuration::default()).await?;

    let subscriber = db.create_subscriber().await?;
    // Subscribe for messages sent to the topic "pong"
    subscriber.subscribe_to("pong").await?;

    // Launch a task that sends out "ping" messages.
    tokio::spawn(pinger(db.clone()));
    // Launch a task that receives "ping" messages and sends "pong" responses.
    tokio::spawn(ponger(db.clone()));

    // Loop until a we receive a message letting us know when the ponger() has
    // no pings remaining.
    loop {
        let message = subscriber.receiver().recv_async().await?;
        let pings_remaining = message.payload::<usize>()?;
        println!(
            "<-- Received {}, pings remaining: {}",
            message.topic, pings_remaining
        );
        if pings_remaining == 0 {
            break;
        }
    }

    println!("Received all pongs.");

    Ok(())
}

async fn pinger<P: PubSub>(pubsub: P) -> anyhow::Result<()> {
    let mut ping_count = 0u32;
    loop {
        ping_count += 1;
        println!("-> Sending ping {}", ping_count);
        pubsub.publish("ping", &ping_count).await?;
        sleep(Duration::from_millis(250)).await;
    }
}

async fn ponger<P: PubSub>(pubsub: P) -> anyhow::Result<()> {
    const NUMBER_OF_PONGS: usize = 5;
    let subscriber = pubsub.create_subscriber().await?;
    subscriber.subscribe_to("ping").await?;
    let mut pings_remaining = NUMBER_OF_PONGS;

    println!(
        "Ponger started, waiting to respond to {} pings",
        pings_remaining
    );

    while pings_remaining > 0 {
        let message = subscriber.receiver().recv_async().await?;
        println!(
            "<- Received {}, id {}",
            message.topic,
            message.payload::<u32>()?
        );
        pings_remaining -= 1;
        pubsub.publish("pong", &pings_remaining).await?;
    }

    println!("Ponger finished.");

    Ok(())
}
