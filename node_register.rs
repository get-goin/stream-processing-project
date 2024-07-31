// interfaces for node registration and exit, definition of node capabilities and location


use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::topology_manager::{TopologyManager, NodeInfo};
use crate::node_metrics::NodeMetrics;

pub struct NodeRegister {
    topology_manager: Arc<TopologyManager>,
    registration_lock: Mutex<()>,
}

pub struct NodeRegistrationRequest {
    pub address: String,
    pub capabilities: NodeCapabilities,
}

pub struct NodeRegistrationResponse {
    pub node_id: Uuid,
    pub registration_time: DateTime<Utc>,
}

#[derive(Error, Debug)]
pub enum NodeRegistrationError {
    #[error("Node registration failed: {0}")]
    RegistrationFailed(String),
    #[error("Node with address {0} already exists")]
    NodeAlreadyExists(String),
    #[error("Topology update failed: {0}")]
    TopologyUpdateFailed(String),
}

impl NodeRegister {
    pub fn new(topology_manager: Arc<TopologyManager>) -> Self {
        Self {
            topology_manager,
            registration_lock: Mutex::new(()),
        }
    }

    pub async fn register_node(&self, request: NodeRegistrationRequest) -> Result<NodeRegistrationResponse, NodeRegistrationError> {
        let _lock = self.registration_lock.lock().await;

        // Optimize the check for existing nodes
        if let Some(existing_node) = self.topology_manager.get_node_by_address(&request.address).await {
            // Handle reregistration case
            self.topology_manager.update_node_capabilities(&existing_node.id, request.capabilities).await
                .map_err(|e| NodeRegistrationError::TopologyUpdateFailed(e.to_string()))?;
            self.topology_manager.update_node_metrics(&existing_node.id, request.initial_metrics).await
                .map_err(|e| NodeRegistrationError::TopologyUpdateFailed(e.to_string()))?;
            return Ok(NodeRegistrationResponse {
                node_id: existing_node.id,
                registration_time: Utc::now(),
            });
        }
    }

    pub async fn deregister_node(&self, node_id: Uuid) -> Result<(), NodeRegistrationError> {
        let _lock = self.registration_lock.lock().await;

        // Check if the node exists
        if self.topology_manager.get_node(&node_id).await.is_err() {
            return Err(NodeRegistrationError::RegistrationFailed(format!("Node with id {} not found", node_id)));
        }

        // Remove the node from the topology manager
        self.topology_manager.remove_node(&node_id)
            .await
            .map_err(|e| NodeRegistrationError::TopologyUpdateFailed(e.to_string()))?;

        Ok(())
    }

    pub async fn update_node_heartbeat(&self, node_id: Uuid) -> Result<(), NodeRegistrationError> {
        self.topology_manager.update_node_heartbeat(&node_id)
            .await
            .map_err(|e| NodeRegistrationError::TopologyUpdateFailed(e.to_string()))
    }

    pub async fn update_node_metrics(&self, node_id: Uuid, metrics: NodeMetrics) -> Result<(), NodeRegistrationError> {
        self.topology_manager.update_node_metrics(&node_id, metrics)
            .await
            .map_err(|e| NodeRegistrationError::TopologyUpdateFailed(e.to_string()))
    }
}