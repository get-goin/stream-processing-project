use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request, Response, Status};
use futures::future::BoxFuture;
use backoff::{ExponentialBackoff, backoff::Backoff};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use edge_stream_proto::{
    node_communication_client::NodeCommunicationClient,
    node_communication_server::{NodeCommunication, NodeCommunicationServer},
    StreamData, TaskAssignment, HeartbeatRequest, HeartbeatResponse, Acknowledgement,
};

#[derive(Clone)]
pub struct NodeCommunicationService {
    connection_pool: Arc<ConnectionPool>,
    topology_manager: TopologyManager,
    stream_processor: Arc<StreamProcessor>,
}

impl NodeCommunicationService {
    pub fn new(max_connections: usize, topology_manager: TopologyManager, stream_processor: Arc<StreamProcessor>) -> Self {
        Self {
            connection_pool: Arc::new(ConnectionPool::new(max_connections)),
            topology_manager,
            stream_processor,
        }
    }

    pub async fn send_stream_data(&self, target_node: &str, stream_data: StreamData) -> Result<(), CommError> {
        let mut client = self.connection_pool.get_connection(target_node).await?;
        let request = Request::new(stream_data);
        let response = client.send_stream_data(request).await?;
        if !response.into_inner().success {
            return Err(CommError::OperationFailed("Failed to send stream data".into()));
        }
        Ok(())
    }

    pub async fn assign_task(&self, target_node: &str, task_assignment: TaskAssignment) -> Result<(), CommError> {
        let mut client = self.connection_pool.get_connection(target_node).await?;
        let request = Request::new(task_assignment);
        let response = client.assign_task(request).await?;
        if !response.into_inner().success {
            return Err(CommError::OperationFailed("Failed to assign task".into()));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl NodeCommunication for NodeCommunicationService {
    async fn send_stream_data(
        &self,
        request: Request<StreamData>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let stream_data = request.into_inner();
        self.stream_processor.process_stream_data(stream_data).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Data received and processed".to_string(),
        }))
    }

    async fn assign_task(
        &self,
        request: Request<TaskAssignment>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let task_assignment = request.into_inner();
        self.stream_processor.assign_task(task_assignment).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Task assigned successfully".to_string(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let heartbeat = request.into_inner();
        let node_id = Uuid::parse_str(&heartbeat.node_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.topology_manager.update_node_heartbeat(&node_id).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
        }))
    }
}

struct ConnectionPool {
    connections: Mutex<HashMap<String, NodeCommunicationClient<Channel>>>,
    max_connections: usize,
}

impl ConnectionPool {
    fn new(max_connections: usize) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            max_connections,
        }
    }

    async fn get_connection(&self, addr: &str) -> Result<NodeCommunicationClient<Channel>, CommError> {
        let mut connections = self.connections.lock().await;
        
        if let Some(client) = connections.get(addr) {
            return Ok(client.clone());
        }

        if connections.len() >= self.max_connections {
            return Err(CommError::PoolExhausted);
        }

        let client = NodeCommunicationClient::connect(addr.to_string()).await?;
        connections.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}

pub async fn run_grpc_server(addr: String, node_communication: NodeCommunicationService) -> Result<(), CommError> {
    let addr = addr.parse()?;

    tonic::transport::Server::builder()
        .add_service(NodeCommunicationServer::new(node_communication))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CommError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),
    #[error("Connection pool exhausted")]
    PoolExhausted,
    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

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