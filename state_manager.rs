use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc, Duration};

// Import other necessary components
use crate::node_communication::NodeCommunicationService;
use crate::topology_manager::TopologyManager;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StateType {
    Local,
    Distributed,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StateMetadata {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
    pub expires_at: Option<DateTime<Utc>>,
}

pub struct StateManager {
    local_store: Arc<dyn StateStore>,
    distributed_store: Arc<dyn StateStore>,
    node_communication: Arc<NodeCommunicationService>,
    topology_manager: Arc<TopologyManager>,
    node_id: Uuid,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
}

impl StateManager {
    pub fn new(
        local_store: Arc<dyn StateStore>,
        distributed_store: Arc<dyn StateStore>,
        node_communication: Arc<NodeCommunicationService>,
        topology_manager: Arc<TopologyManager>,
        node_id: Uuid,
    ) -> Self {
        Self {
            local_store,
            distributed_store,
            node_communication,
            topology_manager,
            node_id,
        }
    }

    pub async fn get<T: for<'de> Deserialize<'de>>(&self, key: &str, state_type: StateType) -> Result<Option<(T, StateMetadata)>> {
        let store = self.get_store(state_type);
        if let Some(bytes) = store.get(key).await? {
            let (metadata, value) = self.deserialize_entry::<T>(&bytes)?;
            if let Some(expires_at) = metadata.expires_at {
                if Utc::now() > expires_at {
                    store.delete(key).await?;
                    return Ok(None);
                }
            }
            Ok(Some((value, metadata)))
        } else if state_type == StateType::Distributed {
            self.fetch_from_other_nodes(key).await
        } else {
            Ok(None)
        }
    }

    pub async fn put<T: Serialize>(&self, key: &str, value: &T, state_type: StateType, ttl: Option<Duration>) -> Result<StateMetadata> {
        let store = self.get_store(state_type);
        let metadata = StateMetadata {
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
            expires_at: ttl.map(|duration| Utc::now() + duration),
        };
        let bytes = self.serialize_entry(&metadata, value)?;
        store.put(key, bytes).await?;

        if state_type == StateType::Distributed {
            self.propagate_to_other_nodes(key, &bytes).await?;
        }

        Ok(metadata)
    }

    pub async fn update<T, F>(&self, key: &str, state_type: StateType, update_fn: F) -> Result<StateMetadata>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(&mut T) -> Result<()>,
    {
        let store = self.get_store(state_type);
        let (mut value, mut metadata) = if let Some((v, m)) = self.get::<T>(key, state_type).await? {
            (v, m)
        } else {
            return Err(anyhow::anyhow!("Key not found"));
        };

        update_fn(&mut value)?;

        metadata.updated_at = Utc::now();
        metadata.version += 1;

        let bytes = self.serialize_entry(&metadata, &value)?;
        store.put(key, bytes).await?;

        if state_type == StateType::Distributed {
            self.propagate_to_other_nodes(key, &bytes).await?;
        }

        Ok(metadata)
    }

    pub async fn delete(&self, key: &str, state_type: StateType) -> Result<()> {
        let store = self.get_store(state_type);
        store.delete(key).await?;

        if state_type == StateType::Distributed {
            self.propagate_delete_to_other_nodes(key).await?;
        }

        Ok(())
    }

    async fn fetch_from_other_nodes<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<(T, StateMetadata)>> {
        let nodes = self.topology_manager.get_active_nodes().await;
        for node in nodes {
            if node.id != self.node_id {
                if let Ok(Some(bytes)) = self.node_communication.query_state(&node.address, key).await {
                    let (metadata, value) = self.deserialize_entry::<T>(&bytes)?;
                    self.distributed_store.put(key, bytes).await?;
                    return Ok(Some((value, metadata)));
                }
            }
        }
        Ok(None)
    }

    async fn propagate_to_other_nodes(&self, key: &str, value: &[u8]) -> Result<()> {
        let nodes = self.topology_manager.get_active_nodes().await;
        for node in nodes {
            if node.id != self.node_id {
                self.node_communication.propagate_state(&node.address, key, value).await?;
            }
        }
        Ok(())
    }

    async fn propagate_delete_to_other_nodes(&self, key: &str) -> Result<()> {
        let nodes = self.topology_manager.get_active_nodes().await;
        for node in nodes {
            if node.id != self.node_id {
                self.node_communication.propagate_delete(&node.address, key).await?;
            }
        }
        Ok(())
    }

    fn get_store(&self, state_type: StateType) -> &Arc<dyn StateStore> {
        match state_type {
            StateType::Local => &self.local_store,
            StateType::Distributed => &self.distributed_store,
        }
    }

    fn serialize_entry<T: Serialize>(&self, metadata: &StateMetadata, value: &T) -> Result<Vec<u8>> {
        let metadata_bytes = bincode::serialize(metadata)?;
        let value_bytes = bincode::serialize(value)?;
        
        let mut bytes = Vec::with_capacity(8 + metadata_bytes.len() + value_bytes.len());
        bytes.extend_from_slice(&(metadata_bytes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&metadata_bytes);
        bytes.extend_from_slice(&value_bytes);
        
        Ok(bytes)
    }

    fn deserialize_entry<T: for<'de> Deserialize<'de>>(&self, bytes: &[u8]) -> Result<(StateMetadata, T)> {
        if bytes.len() < 8 {
            return Err(anyhow::anyhow!("Invalid data format"));
        }

        let metadata_len = u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        
        if bytes.len() < 8 + metadata_len {
            return Err(anyhow::anyhow!("Invalid data format"));
        }

        let metadata: StateMetadata = bincode::deserialize(&bytes[8..8+metadata_len])?;
        let value: T = bincode::deserialize(&bytes[8+metadata_len..])?;

        Ok((metadata, value))
    }

    pub async fn cleanup_expired_state(&self) -> Result<()> {
        for state_type in &[StateType::Local, StateType::Distributed] {
            let store = self.get_store(*state_type);
            let all_keys = store.scan("").await?;
            
            for (key, bytes) in all_keys {
                let (metadata, _) = self.deserialize_entry::<()>(&bytes)?;
                
                if let Some(expires_at) = metadata.expires_at {
                    if Utc::now() > expires_at {
                        store.delete(&key).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

// Helper trait for easy state access
#[async_trait]
pub trait StateAccess {
    fn state_manager(&self) -> &StateManager;

    async fn get_state<T: for<'de> Deserialize<'de>>(&self, key: &str, state_type: StateType) -> Result<Option<T>> {
        self.state_manager().get(key, state_type).await.map(|opt| opt.map(|(v, _)| v))
    }

    async fn set_state<T: Serialize>(&self, key: &str, value: &T, state_type: StateType) -> Result<()> {
        self.state_manager().put(key, value, state_type, None).await.map(|_| ())
    }

    async fn update_state<T, F>(&self, key: &str, state_type: StateType, update_fn: F) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(&mut T) -> Result<()>,
    {
        self.state_manager().update(key, state_type, update_fn).await.map(|_| ())
    }

    async fn delete_state(&self, key: &str, state_type: StateType) -> Result<()> {
        self.state_manager().delete(key, state_type).await
    }
}