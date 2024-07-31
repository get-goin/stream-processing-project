use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use anyhow::Result;

pub struct NodeMetricsCollector {
    node_id: Uuid,
    current_metrics: Arc<RwLock<NodeMetrics>>,
    state_manager: Arc<StateManager>,
    system_stats: SystemStats,
    stream_processor: Arc<StreamProcessor>,
    report_interval: Duration,
}

impl NodeMetricsCollector {
    pub fn new(
        node_id: Uuid,
        state_manager: Arc<StateManager>,
        stream_processor: Arc<StreamProcessor>,
        report_interval: Duration,
    ) -> Self {
        Self {
            node_id,
            current_metrics: Arc::new(RwLock::new(NodeMetrics::default())),
            state_manager,
            system_stats: SystemStats::new(),
            stream_processor,
            report_interval,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = interval(self.report_interval);
    
        loop {
            interval.tick().await;
            match self.collect_and_update_metrics().await {
                Ok(_) => (),
                Err(e) => log::error!("Error collecting metrics: {}", e),
            }
        }
    }
    
    async fn collect_and_update_metrics(&self) -> Result<()> {
        let metrics = self.collect_metrics().await?;
        self.update_metrics(metrics.clone()).await?;
        self.store_metrics().await?;
        self.update_topology_manager(&metrics).await?;
        Ok(())
    }

    async fn collect_metrics(&self) -> Result<NodeMetrics> {
        let cpu_usage = self.system_stats.get_cpu_usage();
        let memory_usage = self.system_stats.get_memory_usage();
        let disk_usage = self.system_stats.get_disk_usage();
        let network_throughput = self.system_stats.get_network_throughput();

        let (active_tasks, queue_lengths, processing_latency, error_count, late_events) = 
            self.state_manager.read(|state| {
                Ok((
                    state.assigned_tasks.len(),
                    state.queue_lengths.clone(),
                    state.processing_latency,
                    state.error_count,
                    state.late_events,
                ))
            }).await?;

        let pipeline_metrics = self.stream_processor.get_pipeline_metrics().await?;

        Ok(NodeMetrics {
            node_id: self.node_id,
            timestamp: Utc::now(),
            cpu_usage,
            memory_usage,
            disk_usage,
            network_throughput,
            active_tasks,
            queue_lengths,
            processing_latency,
            error_count,
            late_events,
            pipeline_metrics,
        })
    }

    async fn update_metrics(&self, metrics: NodeMetrics) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        *current = metrics;
        Ok(())
    }

    async fn store_metrics(&self) -> Result<()> {
        let metrics = self.current_metrics.read().await.clone();
        self.state_manager.put(&format!("metrics:{}", self.node_id), &metrics, StateType::Local).await?;
        Ok(())
    }

    pub async fn get_current_metrics(&self) -> NodeMetrics {
        self.current_metrics.read().await.clone()
    }

    async fn update_topology_manager(&self, metrics: &NodeMetrics) -> Result<()> {
        self.topology_manager.update_node_metrics(&self.node_id, metrics.clone()).await?;
        Ok(())
    }
}

pub struct SystemStats {
    sys: System,
}

impl SystemStats {
    pub fn new() -> Self {
        SystemStats {
            sys: System::new_all(),
        }
    }

    pub fn refresh(&mut self) {
        self.sys.refresh_all();
    }

    pub fn get_cpu_usage(&self) -> f32 {
        self.sys.global_processor_info().cpu_usage()
    }

    pub fn get_memory_usage(&self) -> f32 {
        let total_memory = self.sys.total_memory() as f64;
        let used_memory = self.sys.used_memory() as f64;
        (used_memory / total_memory * 100.0) as f32
    }

    pub fn get_disk_usage(&self) -> f32 {
        let total_space = self.sys.disks().iter().map(|disk| disk.total_space()).sum::<u64>() as f64;
        let used_space = self.sys.disks().iter().map(|disk| disk.total_space() - disk.available_space()).sum::<u64>() as f64;
        (used_space / total_space * 100.0) as f32
    }

    pub fn get_network_throughput(&self) -> f64 {
        self.sys.networks().values().map(|network| network.received() + network.transmitted()).sum::<u64>() as f64
    }
}