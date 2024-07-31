use crate::node_metrics::NodeMetrics;
use crate::node_register::NodeCapabilities;
use crate::stream_processor::StreamProcessor;

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub id: Uuid,
    pub address: String,
    pub last_heartbeat: DateTime<Utc>,
    pub metrics: NodeMetrics,
    pub capabilities: NodeCapabilities,
    pub status: NodeStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeStatus {
    Active,
    Inactive,
    Failing,
}

#[derive(Clone)]
pub struct TopologyManager {
    nodes: Arc<RwLock<HashMap<Uuid, NodeInfo>>>,
    heartbeat_timeout: Duration,
}

impl TopologyManager {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        TopologyManager {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout,
        }
    }

    pub async fn register_node(&self, id: Uuid, address: String, capabilities: NodeCapabilities) -> Result<(), TopologyError> {
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&id) {
            return Err(TopologyError::NodeAlreadyExists(id));
        }
        nodes.insert(id, NodeInfo {
            id,
            address,
            last_heartbeat: Utc::now(),
            metrics: NodeMetrics::default(),
            capabilities,
            status: NodeStatus::Active,
        });
        Ok(())
    }

    pub async fn update_node_heartbeat(&self, id: &Uuid) -> Result<(), TopologyError> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(id) {
            node.last_heartbeat = Utc::now();
            node.status = NodeStatus::Active;
            Ok(())
        } else {
            Err(TopologyError::NodeNotFound(*id))
        }
    }

    pub async fn update_node_metrics(&self, id: &Uuid, metrics: NodeMetrics) -> Result<(), TopologyError> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(id) {
            node.metrics = metrics;
            Ok(())
        } else {
            Err(TopologyError::NodeNotFound(*id))
        }
    }

    pub async fn get_node(&self, id: &Uuid) -> Result<NodeInfo, TopologyError> {
        let nodes = self.nodes.read().await;
        nodes.get(id).cloned().ok_or(TopologyError::NodeNotFound(*id))
    }

    pub async fn get_all_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    pub async fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|node| node.status == NodeStatus::Active)
            .cloned()
            .collect()
    }

    pub async fn remove_inactive_nodes(&self) -> Result<(), TopologyError> {
        let mut nodes = self.nodes.write().await;
        let now = Utc::now();
        nodes.retain(|_, node| {
            let is_active = now - node.last_heartbeat < self.heartbeat_timeout;
            if !is_active {
                node.status = NodeStatus::Inactive;
            }
            is_active
        });
        Ok(())
    }

    pub async fn mark_node_as_failing(&self, id: &Uuid) -> Result<(), TopologyError> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(id) {
            node.status = NodeStatus::Failing;
            Ok(())
        } else {
            Err(TopologyError::NodeNotFound(*id))
        }
    }

    pub async fn get_cluster_metrics(&self) -> HashMap<Uuid, NodeMetrics> {
        let nodes = self.nodes.read().await;
        nodes.iter()
            .map(|(id, node)| (*id, node.metrics.clone()))
            .collect()
    }

    pub async fn get_node_load(&self, id: &Uuid) -> Result<f32, TopologyError> {
        let node = self.get_node(id).await?;
        Ok(node.metrics.cpu_usage)  // Using CPU usage as a simple load indicator
    }

    pub async fn find_least_loaded_node(&self) -> Result<NodeInfo, TopologyError> {
        let nodes = self.get_active_nodes().await;
        nodes.into_iter()
            .min_by(|a, b| a.metrics.cpu_usage.partial_cmp(&b.metrics.cpu_usage).unwrap())
            .ok_or(TopologyError::NoActiveNodes)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("Node not found: {0}")]
    NodeNotFound(Uuid),
    #[error("Node already exists: {0}")]
    NodeAlreadyExists(Uuid),
    #[error("No active nodes in the cluster")]
    NoActiveNodes,
}