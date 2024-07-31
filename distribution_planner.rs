use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use uuid::Uuid;
use petgraph::Graph;
use petgraph::algo::toposort;
use petgraph::visit::EdgeRef;
use tokio::sync::RwLock;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

use crate::pipeline_compiler::{CompiledPipeline, CompiledStage, OperatorType};
use crate::topology_manager::{TopologyManager, NodeInfo, NodeStatus};
use crate::state_manager::{StateManager, StateType};
use crate::metrics_collector::NodeMetrics;
use crate::network_topology::{NetworkTopology, NetworkCost};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributionPlan {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub node_assignments: HashMap<Uuid, Vec<Uuid>>,  // NodeId -> Vec<StageId>
    pub pipeline_partitions: HashMap<Uuid, PipelinePartition>,
    pub shared_state_groups: Vec<HashSet<Uuid>>,
    pub stage_to_partition: HashMap<Uuid, Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelinePartition {
    pub id: Uuid,
    pub stages: Vec<Uuid>,
    pub state_size: usize,
    pub input_rate: f64,
    pub output_rate: f64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub network_usage: f64,
}

pub struct DistributionPlanner {
    topology_manager: Arc<TopologyManager>,
    state_manager: Arc<StateManager>,
    network_topology: Arc<NetworkTopology>,
    distribution_plans: Arc<RwLock<HashMap<Uuid, DistributionPlan>>>,
}

impl DistributionPlanner {
    pub fn new(
        topology_manager: Arc<TopologyManager>,
        state_manager: Arc<StateManager>,
        network_topology: Arc<NetworkTopology>,
    ) -> Self {
        DistributionPlanner {
            topology_manager,
            state_manager,
            network_topology,
            distribution_plans: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_distribution_plan(&self, pipeline: &CompiledPipeline) -> Result<DistributionPlan, DistributionError> {
        let nodes = self.topology_manager.get_active_nodes().await;
        if nodes.is_empty() {
            return Err(DistributionError::InsufficientNodes);
        }

        let pipeline_graph = self.build_pipeline_graph(pipeline);
        let partitions = self.create_pipeline_partitions(&pipeline_graph, pipeline)?;
        let (node_assignments, stage_to_partition) = self.assign_partitions_to_nodes(&partitions, &nodes).await?;
        let shared_state_groups = self.identify_shared_state_groups(pipeline);

        let plan = DistributionPlan {
            id: Uuid::new_v4(),
            pipeline_id: pipeline.id,
            node_assignments,
            pipeline_partitions: partitions.into_iter().map(|p| (p.id, p)).collect(),
            shared_state_groups,
            stage_to_partition,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.distribution_plans.write().await.insert(plan.id, plan.clone());
        Ok(plan)
    }

    fn build_pipeline_graph(&self, pipeline: &CompiledPipeline) -> Graph<&CompiledStage, f64> {
        let mut graph = Graph::new();
        let mut node_map = HashMap::new();

        for stage in &pipeline.stages {
            let node = graph.add_node(stage);
            node_map.insert(stage.id, node);
        }

        for stage in &pipeline.stages {
            for input in &stage.inputs {
                if let Some(&input_node) = node_map.get(&input) {
                    let current_node = node_map[&stage.id];
                    let data_rate = self.estimate_data_rate(stage);
                    graph.add_edge(input_node, current_node, data_rate);
                }
            }
        }

        graph
    }

    fn create_pipeline_partitions(&self, graph: &Graph<&CompiledStage, f64>, pipeline: &CompiledPipeline) 
        -> Result<Vec<PipelinePartition>, DistributionError> {
        let sorted_stages = toposort(&graph, None)
            .map_err(|_| DistributionError::CyclicPipeline)?;

        let mut partitions = Vec::new();
        let mut current_partition = PipelinePartition {
            id: Uuid::new_v4(),
            stages: Vec::new(),
            state_size: 0,
            input_rate: 0.0,
            output_rate: 0.0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            network_usage: 0.0,
        };

        for node_index in sorted_stages {
            let stage = graph[node_index];
            
            if self.should_start_new_partition(&current_partition, stage) {
                if !current_partition.stages.is_empty() {
                    partitions.push(current_partition);
                }
                current_partition = PipelinePartition {
                    id: Uuid::new_v4(),
                    stages: Vec::new(),
                    state_size: 0,
                    input_rate: 0.0,
                    output_rate: 0.0,
                    cpu_usage: 0.0,
                    memory_usage: 0.0,
                    network_usage: 0.0,
                };
            }

            current_partition.stages.push(stage.id);
            current_partition.state_size += self.estimate_stage_state_size(stage);
            current_partition.input_rate += self.estimate_stage_input_rate(stage);
            current_partition.output_rate += self.estimate_stage_output_rate(stage);
            current_partition.cpu_usage += self.estimate_stage_cpu_usage(stage);
            current_partition.memory_usage += self.estimate_stage_memory_usage(stage);
            current_partition.network_usage += self.estimate_stage_network_usage(stage);
        }

        if !current_partition.stages.is_empty() {
            partitions.push(current_partition);
        }

        Ok(partitions)
    }

    async fn assign_partitions_to_nodes(&self, partitions: &[PipelinePartition], nodes: &[NodeInfo]) 
        -> Result<(HashMap<Uuid, Vec<Uuid>>, HashMap<Uuid, Uuid>), DistributionError> {
        let mut node_assignments = HashMap::new();
        let mut stage_to_partition = HashMap::new();
        let mut node_loads = nodes.iter().map(|n| (n.id, NodeLoad::default())).collect::<HashMap<_, _>>();

        for partition in partitions {
            let best_node = self.find_best_node_for_partition(partition, &node_loads, nodes).await?;
            node_assignments.entry(best_node).or_insert_with(Vec::new).extend(partition.stages.iter().cloned());
            for &stage_id in &partition.stages {
                stage_to_partition.insert(stage_id, partition.id);
            }
            node_loads.get_mut(&best_node).unwrap().update(partition);
        }

        Ok((node_assignments, stage_to_partition))
    }

    fn identify_shared_state_groups(&self, pipeline: &CompiledPipeline) -> Vec<HashSet<Uuid>> {
        let mut groups = Vec::new();
        let mut stage_to_group = HashMap::new();

        for (i, stage) in pipeline.stages.iter().enumerate() {
            if self.is_stateful_operation(stage) {
                let mut group = HashSet::new();
                group.insert(stage.id);

                for other_stage in &pipeline.stages[i+1..] {
                    if self.shares_state(stage, other_stage) {
                        group.insert(other_stage.id);
                    }
                }

                if group.len() > 1 {
                    for &stage_id in &group {
                        stage_to_group.insert(stage_id, groups.len());
                    }
                    groups.push(group);
                }
            }
        }

        groups
    }

    fn should_start_new_partition(&self, current_partition: &PipelinePartition, stage: &CompiledStage) -> bool {
        current_partition.state_size > 1_000_000_000 || // 1GB state size limit
        current_partition.stages.len() >= 10 || // Max 10 stages per partition
        self.is_shuffle_operation(stage) || // Start new partition after shuffle operations
        current_partition.cpu_usage > 80.0 || // CPU usage limit
        current_partition.memory_usage > 80.0 // Memory usage limit
    }

    fn estimate_stage_state_size(&self, stage: &CompiledStage) -> usize {
        match &stage.operator {
            OperatorType::Window(_) | OperatorType::Join(_) => 1_000_000, // Assume 1MB state for stateful operations
            OperatorType::Aggregate(_) => 500_000, // Assume 500KB state for aggregations
            _ => 0, // Assume no state for other operations
        }
    }

    fn estimate_stage_input_rate(&self, stage: &CompiledStage) -> f64 {
        // This would ideally come from historical metrics or user-provided estimates
        100.0 // Assume 100 events/second as a placeholder
    }

    fn estimate_stage_output_rate(&self, stage: &CompiledStage) -> f64 {
        let input_rate = self.estimate_stage_input_rate(stage);
        match &stage.operator {
            OperatorType::Filter(_) => input_rate * 0.5, // Assume 50% of events pass the filter
            OperatorType::Map(_) => input_rate, // 1:1 input to output ratio
            OperatorType::FlatMap(_) => input_rate * 1.5, // Assume each input produces 1.5 outputs on average
            OperatorType::Aggregate(_) | OperatorType::Window(_) => input_rate * 0.1, // Assume significant reduction
            OperatorType::Join(_) => input_rate * 0.8, // Assume some reduction due to join condition
            _ => input_rate,
        }
    }

    async fn find_best_node_for_partition(&self, partition: &PipelinePartition, node_loads: &HashMap<Uuid, NodeLoad>, nodes: &[NodeInfo]) 
        -> Result<Uuid, DistributionError> {
        let mut best_node = None;
        let mut min_cost = f64::MAX;

        for node in nodes {
            if self.has_sufficient_resources(node, partition) {
                let cost = self.calculate_assignment_cost(node, partition, node_loads).await;
                if cost < min_cost {
                    min_cost = cost;
                    best_node = Some(node.id);
                }
            }
        }

        best_node.ok_or(DistributionError::InsufficientResources)
    }

    fn has_sufficient_resources(&self, node: &NodeInfo, partition: &PipelinePartition) -> bool {
        node.metrics.cpu_usage + partition.cpu_usage <= 90.0 &&
        node.metrics.memory_usage + partition.memory_usage <= 90.0 &&
        node.metrics.network_throughput + partition.network_usage <= node.capabilities.network_mbps as f64
    }

    async fn calculate_assignment_cost(&self, node: &NodeInfo, partition: &PipelinePartition, node_loads: &HashMap<Uuid, NodeLoad>) -> f64 {
        let load_cost = node_loads.get(&node.id).map(|load| load.total_load()).unwrap_or(0.0);
        let network_cost = self.calculate_network_cost(node, partition).await;
        let data_locality_cost = self.calculate_data_locality_cost(node, partition).await;

        0.5 * load_cost + 0.3 * network_cost + 0.2 * data_locality_cost
    }

    async fn calculate_network_cost(&self, node: &NodeInfo, partition: &PipelinePartition) -> f64 {
        let mut cost = 0.0;
        for &stage_id in &partition.stages {
            if let Some(stage) = self.state_manager.get_stage(stage_id).await {
                for &input_stage_id in &stage.inputs {
                    if let Some(input_node_id) = self.get_node_for_stage(input_stage_id).await {
                        if input_node_id != node.id {
                            cost += self.network_topology.get_cost(input_node_id, node.id).await;
                        }
                    }
                }
            }
        }
        cost
    }

    async fn calculate_data_locality_cost(&self, node: &NodeInfo, partition: &PipelinePartition) -> f64 {
        let mut cost = 0.0;
        for &stage_id in &partition.stages {
            if let Some(stage) = self.state_manager.get_stage(stage_id).await {
                for &input_stage_id in &stage.inputs {
                    if let Some(data_location) = self.state_manager.get_data_location(input_stage_id).await {
                        if data_location != node.id {
                            cost += 1.0;
                        }
                    }
                }
            }
        }
        cost
    }

    async fn get_node_for_stage(&self, stage_id: Uuid) -> Option<Uuid> {
        for plan in self.distribution_plans.read().await.values() {
            for (&node_id, stages) in &plan.node_assignments {
                if stages.contains(&stage_id) {
                    return Some(node_id);
                }
            }
        }
        None
    }

    fn estimate_stage_cpu_usage(&self, stage: &CompiledStage) -> f64 {
        match &stage.operator {
            OperatorType::Filter(_) => 5.0,
            OperatorType::Map(_) => 10.0,
            OperatorType::FlatMap(_) => 15.0,
            OperatorType::Aggregate(_) => 20.0,
            OperatorType::Window(_) => 25.0,
            OperatorType::Join(_) => 30.0,
            _ => 5.0,
        }
    }

    fn estimate_stage_memory_usage(&self, stage: &CompiledStage) -> f64 {
        match &stage.operator {
            OperatorType::Window(_) | OperatorType::Join(_) => 20.0,
            OperatorType::Aggregate(_) => 15.0,
            _ => 5.0,
        }
    }

    fn estimate_stage_network_usage(&self, stage: &CompiledStage) -> f64 {
        self.estimate_stage_output_rate(stage) * self.estimate_average_event_size(stage)
    }

    fn estimate_average_event_size(&self, stage: &CompiledStage) -> f64 {
        // This would ideally be based on schema information or historical data
        1024.0 // Assume 1KB per event as a placeholder
    }

    fn is_stateful_operation(&self, stage: &CompiledStage) -> bool {
        matches!(stage.operator, OperatorType::Window(_) | OperatorType::Join(_) | OperatorType::Aggregate(_))
    }

    fn shares_state(&self, stage1: &CompiledStage, stage2: &CompiledStage) -> bool {
        // This is a simplified implementation. In a real system, you'd need to analyze
        // the specific state being used by each operator.
        match (&stage1.operator, &stage2.operator) {
            (OperatorType::Window(w1), OperatorType::Window(w2)) => w1.window_type == w2.window_type,
            (OperatorType::Join(j1), OperatorType::Join(j2)) => j1.join_key == j2.join_key,
            (OperatorType::Aggregate(a1), OperatorType::Aggregate(a2)) => a1.group_by_key == a2.group_by_key,
            _ => false,
        }
    }

    fn is_shuffle_operation(&self, stage: &CompiledStage) -> bool {
        matches!(stage.operator, OperatorType::Repartition(_))
    }

    fn estimate_data_rate(&self, stage: &CompiledStage) -> f64 {
        self.estimate_stage_output_rate(stage) * self.estimate_average_event_size(stage)
    }

    pub async fn rebalance_distribution(&self, pipeline_id: Uuid) -> Result<DistributionPlan, DistributionError> {
        let mut plans = self.distribution_plans.write().await;
        let plan = plans.get_mut(&pipeline_id).ok_or(DistributionError::PlanNotFound)?;

        let nodes = self.topology_manager.get_active_nodes().await;
        let mut node_loads = HashMap::new();
        for (node_id, stages) in &plan.node_assignments {
            let node_info = self.topology_manager.get_node(node_id).await.ok_or(DistributionError::NodeNotFound(*node_id))?;
            let load = self.calculate_node_load(&node_info, stages).await?;
            node_loads.insert(*node_id, load);
        }

        let overloaded_nodes: Vec<_> = node_loads.iter()
            .filter(|(_, load)| load.is_overloaded())
            .map(|(&id, _)| id)
            .collect();

        for node_id in overloaded_nodes {
            self.rebalance_node(node_id, &mut plan.node_assignments, &mut node_loads, &nodes).await?;
        }

        plan.updated_at = Utc::now();
        Ok(plan.clone())
    }

    async fn rebalance_node(
        &self,
        node_id: Uuid,
        node_assignments: &mut HashMap<Uuid, Vec<Uuid>>,
        node_loads: &mut HashMap<Uuid, NodeLoad>,
        nodes: &[NodeInfo],
    ) -> Result<(), DistributionError> {
        let stages_to_move = node_assignments.get(&node_id).ok_or(DistributionError::NodeNotFound(node_id))?;
        for &stage_id in stages_to_move {
            let stage = self.state_manager.get_stage(stage_id).await.ok_or(DistributionError::StageNotFound(stage_id))?;
            let partition = self.create_partition_for_stage(&stage);
            let best_node = self.find_best_node_for_partition(&partition, node_loads, nodes).await?;
            
            // Move the stage to the new node
            node_assignments.get_mut(&node_id).unwrap().retain(|&s| s != stage_id);
            node_assignments.entry(best_node).or_default().push(stage_id);

            // Update node loads
            node_loads.get_mut(&node_id).unwrap().remove_partition(&partition);
            node_loads.get_mut(&best_node).unwrap().update(&partition);
        }
        Ok(())
    }

    fn create_partition_for_stage(&self, stage: &CompiledStage) -> PipelinePartition {
        PipelinePartition {
            id: Uuid::new_v4(),
            stages: vec![stage.id],
            state_size: self.estimate_stage_state_size(stage),
            input_rate: self.estimate_stage_input_rate(stage),
            output_rate: self.estimate_stage_output_rate(stage),
            cpu_usage: self.estimate_stage_cpu_usage(stage),
            memory_usage: self.estimate_stage_memory_usage(stage),
            network_usage: self.estimate_stage_network_usage(stage),
        }
    }

    async fn calculate_node_load(&self, node: &NodeInfo, stages: &[Uuid]) -> Result<NodeLoad, DistributionError> {
        let mut load = NodeLoad::default();
        for &stage_id in stages {
            let stage = self.state_manager.get_stage(stage_id).await.ok_or(DistributionError::StageNotFound(stage_id))?;
            let partition = self.create_partition_for_stage(&stage);
            load.update(&partition);
        }
        Ok(load)
    }
}

#[derive(Debug, Clone, Default)]
struct NodeLoad {
    cpu_usage: f64,
    memory_usage: f64,
    network_usage: f64,
}

impl NodeLoad {
    fn update(&mut self, partition: &PipelinePartition) {
        self.cpu_usage += partition.cpu_usage;
        self.memory_usage += partition.memory_usage;
        self.network_usage += partition.network_usage;
    }

    fn remove_partition(&mut self, partition: &PipelinePartition) {
        self.cpu_usage -= partition.cpu_usage;
        self.memory_usage -= partition.memory_usage;
        self.network_usage -= partition.network_usage;
    }

    fn total_load(&self) -> f64 {
        self.cpu_usage + self.memory_usage + self.network_usage
    }

    fn is_overloaded(&self) -> bool {
        self.cpu_usage > 90.0 || self.memory_usage > 90.0 || self.network_usage > 90.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DistributionError {
    #[error("Insufficient nodes available")]
    InsufficientNodes,
    #[error("Insufficient resources across all nodes")]
    InsufficientResources,
    #[error("Unable to assign all stages")]
    IncompleteAssignment,
    #[error("Cyclic dependency detected in pipeline")]
    CyclicPipeline,
    #[error("Distribution plan not found")]
    PlanNotFound,
    #[error("Node not found: {0}")]
    NodeNotFound(Uuid),
    #[error("Stage not found: {0}")]
    StageNotFound(Uuid),
    #[error("State manager error: {0}")]
    StateManagerError(#[from] crate::state_manager::StateError),
    #[error("Topology manager error: {0}")]
    TopologyManagerError(#[from] crate::topology_manager::TopologyError),
}

// Implement additional methods for DistributionPlan
impl DistributionPlan {
    pub fn get_node_for_stage(&self, stage_id: Uuid) -> Option<Uuid> {
        self.node_assignments.iter()
            .find(|(_, stages)| stages.contains(&stage_id))
            .map(|(&node_id, _)| node_id)
    }

    pub fn get_stages_for_node(&self, node_id: Uuid) -> Option<&Vec<Uuid>> {
        self.node_assignments.get(&node_id)
    }

    pub fn get_partition_for_stage(&self, stage_id: Uuid) -> Option<&PipelinePartition> {
        self.stage_to_partition.get(&stage_id)
            .and_then(|partition_id| self.pipeline_partitions.get(partition_id))
    }
}

// Add serialization support for DistributionPlan
impl Serialize for DistributionPlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Implement serialization logic
    }
}

// Add deserialization support for DistributionPlan
impl<'de> Deserialize<'de> for DistributionPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Implement deserialization logic
    }
}