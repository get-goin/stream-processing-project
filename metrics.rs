pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

pub enum MetricsType {
    NodeMetrics,
    PipelineMetrics,
    SystemWideMetrics,
}

pub struct ClusterMetricsResponse {
    pub node_metrics: HashMap<Uuid, Vec<NodeMetrics>>,
    pub pipeline_metrics: HashMap<Uuid, Vec<PipelineMetrics>>,
    pub system_metrics: SystemMetrics,
}

pub struct ClusterMetricsRequest {
    pub time_range: TimeRange,
    pub metrics_type: MetricsType,
}

pub struct NodeCapabilities {
    pub cpu_cores: u32,
    pub memory_gb: u64,
    pub disk_gb: u64,
    pub network_mbps: u64,
}

pub struct NodeMetrics {
    pub node_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub disk_usage: f32,
    pub network_throughput: f64,
    pub active_tasks: usize,
}

pub struct PipelineMetrics {
    pub pipeline_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub throughput: f64,
    pub latency: Duration,
    pub error_rate: f32,
    pub stage_metrics: HashMap<Uuid, StageMetrics>,
}

pub struct StageMetrics {
    pub stage_id: Uuid,
    pub processed_events: u64,
    pub processing_time: Duration,
    pub error_count: u64,
}

pub struct SystemMetrics {
    pub total_nodes: usize,
    pub total_pipelines: usize,
    pub total_throughput: f64,
    pub average_latency: Duration,
    pub error_rate: f32,
}



