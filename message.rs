enum Message {
    Heartbeat(Heartbeat),
    TaskAssignment(TaskAssignment),
    MetricsReport(MetricsReport),
    StreamData(StreamMessage),
    PipelineUpdate(PipelineUpdate),
    ConsensusProposal(ConsensusProposal),
    ConsensusVote(ConsensusVote),
}

struct Heartbeat {
    node_id: Uuid,
    timestamp: DateTime<Utc>,
}

struct TaskAssignment {
    node_id: Uuid,
    task_id: TaskId,
    pipeline_stage: Stage,
}

struct MetricsReport {
    node_id: Uuid,
    metrics: NodeMetrics,
    timestamp: DateTime<Utc>,
}

struct PipelineUpdate {
    pipeline_id: Uuid,
    update_type: PipelineUpdateType,
    details: String,
}

enum PipelineUpdateType {
    Created,
    Modified,
    Deleted,
}

struct ConsensusProposal {
    proposal_id: Uuid,
    proposer_id: Uuid,
    proposal: Vec<u8>,
}

struct ConsensusVote {
    proposal_id: Uuid,
    voter_id: Uuid,
    vote: bool,
}