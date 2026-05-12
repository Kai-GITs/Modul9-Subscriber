use borsh::{BorshDeserialize, BorshSerialize};
use futures::stream::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct UserCreatedEventMessage {
    pub user_id: String,
    pub user_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection =
        Connection::connect("amqp://guest:guest@localhost:5672", ConnectionProperties::default())
            .await?;
    let channel = connection.create_channel().await?;

    channel
        .queue_declare(
            "user_created".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "user_created".into(),
            "subscriber".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let message = UserCreatedEventMessage::try_from_slice(&delivery.data)?;

        println!(
            "In Kalfin's Computer [2406360256]. Message received: {:?}",
            message
        );

        delivery.ack(BasicAckOptions::default()).await?;
    }

    Ok(())
}
