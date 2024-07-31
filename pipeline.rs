use uuid::Uuid;
use std::collections::HashMap;

pub struct Pipeline {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub stages: Vec<Stage>,
    pub sources: Vec<Source>,
    pub sinks: Vec<Sink>,
    pub metadata: HashMap<String, String>,
}

pub struct Stage {
    pub id: Uuid,
    pub name: String,
    pub operation: Operation,
    pub inputs: Vec<StageInput>,
    pub outputs: Vec<StageOutput>,
}

pub struct Source {
    pub id: Uuid,
    pub name: String,
    pub source_type: SourceType,
    pub schema: Schema,
    pub config: HashMap<String, String>,
}

pub struct Sink {
    pub id: Uuid,
    pub name: String,
    pub sink_type: SinkType,
    pub schema: Schema,
    pub config: HashMap<String, String>,
}

pub enum Operation {
    Filter(FilterOperation),
    Map(MapOperation),
    Aggregate(AggregateOperation),
    Join(JoinOperation),
    Window(WindowOperation),
    CEP(CEPOperation),
    Shuffle(ShuffleOperation),
}

pub struct StreamDefinition {
    pub id: Uuid,
    pub name: String,
    pub schema: Schema,
}

pub struct Schema {
    pub fields: Vec<Field>,
}

pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

pub enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    Timestamp,
    Array(Box<DataType>),
    Struct(HashMap<String, DataType>),
}

pub struct StageInput {
    pub source: SourceType,
    pub schema: Schema,
}

pub enum SourceType {
    Stream(Uuid),
    Stage(Uuid),
}

pub struct StageOutput {
    pub stream_id: Uuid,
    pub schema: Schema,
}
