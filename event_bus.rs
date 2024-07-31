use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use futures::Stream;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use anyhow::Result;

use crate::state_manager::{StateManager, StateAccess, StateType, StateError};
use crate::stream_processor::StreamProcessor;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub source_node: Uuid,
    pub timestamp: DateTime<Utc>,
}

pub struct EventBus {
    state_manager: Arc<StateManager>,
    channels: RwLock<std::collections::HashMap<String, broadcast::Sender<Event>>>,
    max_events_per_topic: usize,
    stream_processor: Arc<StreamProcessor>,
}

#[async_trait::async_trait]
impl StateAccess for EventBus {
    fn state_manager(&self) -> &StateManager {
        &self.state_manager
    }
}

impl EventBus {
    pub fn new(state_manager: Arc<StateManager>, stream_processor: Arc<StreamProcessor>, max_events_per_topic: usize) -> Self {
        EventBus {
            state_manager,
            channels: RwLock::new(std::collections::HashMap::new()),
            max_events_per_topic,
            stream_processor,
        }
    }

    pub async fn publish(&self, event: Event) -> Result<(), StateError> {
        // Store the event in the state manager
        let key = format!("events:{}:{}", event.topic, event.timestamp);
        self.set_state(&key, &event, StateType::Local).await?;

        // Cleanup old events if necessary
        self.cleanup_old_events(&event.topic).await?;

        // Publish to subscribers
        if let Some(sender) = self.channels.read().await.get(&event.topic) {
            let _ = sender.send(event.clone());
        }

        Ok(())
    }

    pub async fn create_stream(&self, topic: String) -> impl Stream<Item = Event> {
        let channels = self.channels.read().await;
        let sender = channels
            .get(&topic)
            .cloned()
            .unwrap_or_else(|| {
                let (tx, _) = broadcast::channel(100);
                tx
            });
        
        BroadcastStream::new(sender.subscribe())
            .filter_map(Result::ok)
    }

    pub async fn register_stream_for_processing(&self, topic: String, pipeline_id: Uuid) -> Result<(), anyhow::Error> {
        let stream = self.create_stream(topic.clone()).await;
        let data_point_stream = stream.map(|event| -> Result<DataPoint, anyhow::Error> {
            Ok(DataPoint {
                id: event.id,
                timestamp: event.timestamp,
                stream_id: Uuid::new_v4(), // You might want to derive this from the topic or event
                values: serde_json::from_slice(&event.payload)?,
            })
        });
        self.stream_processor.process_stream(pipeline_id, data_point_stream).await?;
        Ok(())
    }

    async fn cleanup_old_events(&self, topic: &str) -> Result<(), StateError> {
        let prefix = format!("events:{}:", topic);
        let events = self.state_manager.scan::<Event>(&prefix, StateType::Local).await?;

        if events.len() > self.max_events_per_topic {
            let events_to_remove = events.len() - self.max_events_per_topic;
            for (key, _, _) in events.iter().take(events_to_remove) {
                self.state_manager.delete(key, StateType::Local).await?;
            }
        }

        Ok(())
    }

    pub async fn subscribe<F>(&self, topic: String, mut callback: F) -> Result<(), anyhow::Error>
    where
        F: FnMut(&Event) + Send + 'static,
    {
        let mut stream = self.create_stream(topic).await;
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                callback(&event);
            }
        });
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<(), anyhow::Error> {
        self.channels.write().await.remove(topic);
        Ok(())
    }
}