use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

struct SharedStateManager {
    state_manager: Arc<StateManager>,
    sharing_group: RwLock<HashSet<NodeId>>,
    node_communicator: Arc<NodeCommunicator>,
}

impl SharedStateManager {
    fn new(state_manager: Arc<StateManager>, node_communicator: Arc<NodeCommunicator>) -> Self {
        Self {
            state_manager,
            sharing_group: RwLock::new(HashSet::new()),
            node_communicator,
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StateError> {
        // First, try to get from local StateManager
        if let Some(value) = self.state_manager.get(key).await? {
            return Ok(Some(value));
        }

        // If not found locally, query other nodes in sharing group
        let sharing_group = self.sharing_group.read().await;
        for &node_id in sharing_group.iter() {
            if let Some(value) = self.node_communicator.query_state(node_id, key).await? {
                // Update local state for future queries
                self.state_manager.put(key, value.clone()).await?;
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StateError> {
        // Update local state
        self.state_manager.put(key, value.clone()).await?;

        // Propagate to other nodes in sharing group
        let sharing_group = self.sharing_group.read().await;
        for &node_id in sharing_group.iter() {
            self.node_communicator.propagate_state(node_id, key, &value).await?;
        }

        Ok(())
    }

    async fn join_sharing_group(&self, group_id: Uuid) -> Result<(), StateError> {
        let mut sharing_group = self.sharing_group.write().await;
        sharing_group.insert(group_id);
        Ok(())
    }
}