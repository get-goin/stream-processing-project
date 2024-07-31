use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use uuid::Uuid;
use anyhow::Result;

use crate::stream_processor::{StreamProcessor, Pipeline};
use crate::state_manager::{StateManager, StateAccess, StateError};
use crate::event_bus::{EventBus, Event};
use crate::metrics_collector::NodeMetricsCollector;
use crate::time_manager::TimeManager;
use crate::node_communication::{NodeCommunication, NodeCommunicationService};
use crate::topology_manager::NodeStatus;

pub struct NodeRuntime {
    id: Uuid,
    state_manager: Arc<StateManager>,
    stream_processor: Arc<StreamProcessor>,
    event_bus: Arc<EventBus>,
    metrics_collector: Arc<Mutex<NodeMetricsCollector>>,
    time_manager: Arc<TimeManager>,
    node_communication: Arc<NodeCommunication>,
    assigned_tasks: Vec<TaskId>,
    shutdown_signal: mpsc::Sender<()>,
}

impl NodeRuntime {
    pub async fn new(
        id: Uuid, 
        capabilities: NodeCapabilities,
        metrics_interval: Duration,
        db_path: &str,
        grpc_addr: String,
    ) -> Result<Self, NodeInitializationError> {
        let (shutdown_tx, _) = mpsc::channel(1);
        let (metrics_tx, _) = mpsc::channel(100);
        
        let state_manager = Arc::new(StateManager::new(db_path)?);
        let time_manager = Arc::new(TimeManager::new(Duration::from_secs(10))); // Example max out-of-orderness
        let node_communication = Arc::new(NodeCommunication::new(id, 100)); // Adjust max connections as needed

        let metrics_collector = Arc::new(Mutex::new(NodeMetricsCollector::new(
            metrics_interval,
            state_manager.clone(),
            metrics_tx,
        )));

        let stream_processor = Arc::new(StreamProcessor::new(
            state_manager.clone(),
            time_manager.clone(),
            node_communication.clone(),
            metrics_collector.clone(),
        ));

        let event_bus = Arc::new(EventBus::new(
            state_manager.clone(),
            stream_processor.clone(),
            1000, // max_events_per_topic
        ));

        let grpc_server_handle = tokio::spawn(run_grpc_server(grpc_addr, NodeCommunicationService::new(
            stream_processor.clone(),
            event_bus.clone(),
            state_manager.clone(),
        )));

        Ok(NodeRuntime {
            id,
            state_manager,
            stream_processor,
            event_bus,
            metrics_collector,
            time_manager,
            node_communication,
            assigned_tasks: Vec::new(),
            shutdown_signal: shutdown_tx,
        })
    }

    pub async fn run(&self) -> Result<(), NodeRuntimeError> {
        self.update_status(NodeStatus::Running).await?;
    
        let stream_processor = self.stream_processor.clone();
        let event_bus = self.event_bus.clone();
        let metrics_collector = self.metrics_collector.clone();
    
        let mut shutdown_rx = self.shutdown_signal.subscribe();
    
        let metrics_task = tokio::spawn(async move {
            loop {
                if let Err(e) = metrics_collector.lock().await.run().await {
                    eprintln!("Metrics collector error: {}", e);
                    break;
                }
            }
        });

        let cleanup_windows_task = {
            let stream_processor = stream_processor.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes
                loop {
                    interval.tick().await;
                    if let Err(e) = stream_processor.cleanup_windows().await {
                        eprintln!("Error during window cleanup: {}", e);
                    }
                }
            })
        };

        tokio::select! {
            result = self.process_events(stream_processor.clone(), event_bus) => {
                result?;
            }
            _ = metrics_task => {
                eprintln!("Metrics collection task ended unexpectedly");
            }
            _ = cleanup_windows_task => {
                eprintln!("Window cleanup task ended unexpectedly");
            }
            _ = shutdown_rx.recv() => {
                self.update_status(NodeStatus::ShuttingDown).await?;
            }
        }
    
        self.shutdown().await?;
        Ok(())
    }

    async fn process_events(
        &self,
        stream_processor: Arc<StreamProcessor>,
        event_bus: Arc<EventBus>,
    ) -> Result<(), StreamProcessingError> {
        loop {
            let event = event_bus.next_event().await?;
            match event {
                Event::StreamData(data_point) => {
                    let pipeline_id = self.determine_pipeline(&data_point);
                    if let Err(e) = stream_processor.process_data_point(pipeline_id, &self.pipelines.read().await[&pipeline_id], data_point).await {
                        eprintln!("Error processing data point: {:?}", e);
                    }
                }
                Event::NewStream(stream) => {
                    stream_processor.add_stream(stream).await?;
                    self.state_manager.update(|state| {
                        state.assigned_tasks.push(stream.id);
                        Ok(())
                    }).await?;
                }
                Event::NewPipeline(pipeline) => {
                    stream_processor.add_pipeline(pipeline).await?;
                }
                // Handle other event types...
            }
        }
    }

    fn determine_pipeline(&self, data_point: &DataPoint) -> Uuid {
        // Logic to determine which pipeline should process this data point
        // This could be based on the data point's content, metadata, or predefined rules
        unimplemented!()
    }

    pub async fn register_pipeline(&self, pipeline: Pipeline) -> Result<(), NodeRuntimeError> {
        self.stream_processor.add_pipeline(pipeline).await?;
        Ok(())
    }

    async fn update_status(&self, new_status: NodeStatus) -> Result<(), StateError> {
        self.state_manager.update(|state| {
            state.status = new_status;
            Ok(())
        }).await
    }

    async fn shutdown(&self) -> Result<(), ShutdownError> {
        // Stop the metrics collector
        if let Ok(mut collector) = self.metrics_collector.lock().await {
            collector.stop().await?;
        }
    
        // Implement other graceful shutdown logic
        Ok(())
    }
}

async fn run_grpc_server(addr: String, service: NodeCommunicationService) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(NodeCommunicationServer::new(service))
        .serve(addr.parse()?)
        .await
}