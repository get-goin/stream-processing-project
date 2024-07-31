use std::sync::Arc;
use tokio::sync::RwLock;
use futures::{Stream, StreamExt};
use uuid::Uuid;
use anyhow::{Result, Context};
use async_trait::async_trait;
use std::collections::HashMap;
use chrono::Duration;

use crate::state_manager::{StateManager, StateAccess, StateType, StateError};
use crate::time_manager::TimeManager;
use crate::operators::{Operator, DataPoint, Value};

pub struct StreamProcessor {
    state_manager: Arc<StateManager>,
    time_manager: Arc<TimeManager>,
    pipelines: RwLock<HashMap<Uuid, Pipeline>>,
}

#[async_trait]
impl StateAccess for StreamProcessor {
    fn state_manager(&self) -> &StateManager {
        &self.state_manager
    }
}

impl StreamProcessor {
    pub fn new(state_manager: Arc<StateManager>, time_manager: Arc<TimeManager>) -> Self {
        StreamProcessor {
            state_manager,
            time_manager,
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    pub async fn process_stream<S>(&self, pipeline_id: Uuid, stream: S) -> Result<(), anyhow::Error>
    where
        S: Stream<Item = Result<DataPoint, anyhow::Error>> + Send + 'static,
    {
        let pipeline = self.pipelines.read().await.get(&pipeline_id).cloned()
            .context(format!("Pipeline not found: {}", pipeline_id))?;

        let mut stream = Box::pin(stream);
        while let Some(data_point_result) = stream.next().await {
            let data_point = data_point_result?;
            if let Err(e) = self.process_data_point(pipeline_id, &pipeline, data_point).await {
                eprintln!("Error processing data point: {:?}", e);
            }
        }

        Ok(())
    }

    async fn process_data_point(&self, pipeline_id: Uuid, pipeline: &Pipeline, data_point: DataPoint) -> Result<(), anyhow::Error> {
        self.time_manager.update_watermark(data_point.timestamp).await;

        if !self.time_manager.handle_late_event(data_point.timestamp).await {
            // Handle late event (e.g., side output or logging)
            log::warn!("Late event detected: {:?}", data_point);
            return Ok(());
        }

        let mut current_data = vec![data_point];

        for stage in &pipeline.stages {
            current_data = self.apply_operator(pipeline_id, stage, current_data).await?;
            if current_data.is_empty() {
                break;
            }
        }

        // Store the final processed data points
        for dp in current_data {
            let key = format!("processed:{}:{}", pipeline_id, dp.id);
            self.set_state_with_error_handling(&key, &dp, StateType::Local).await?;
        }

        Ok(())
    }

    async fn apply_operator(&self, pipeline_id: Uuid, stage: &PipelineStage, data_points: Vec<DataPoint>) -> Result<Vec<DataPoint>, anyhow::Error> {
        match &stage.operator {
            Operator::Filter(filter) => {
                Ok(data_points.into_iter().filter(|dp| filter.apply(dp)).collect())
            },
            Operator::Map(map) => {
                data_points.into_iter().map(|dp| map.apply(dp)).collect()
            },
            Operator::Window(window) => {
                let key = format!("window:{}:{}", pipeline_id, stage.id);
                self.state_manager.update(&key, StateType::Local, |state: &mut Vec<DataPoint>| {
                    state.extend(data_points);
                    if window.should_emit(state, self.time_manager.get_current_watermark().await) {
                        let result = window.apply(std::mem::take(state))?;
                        Ok(vec![result])
                    } else {
                        Ok(vec![])
                    }
                }).await.context("Window operation failed")?
            },
            Operator::Join(join) => {
                let key = format!("join:{}:{}", pipeline_id, stage.id);
                self.state_manager.update(&key, StateType::Local, |state: &mut (Vec<DataPoint>, Vec<DataPoint>)| {
                    for dp in data_points {
                        if dp.values.contains_key(&join.left_key) {
                            state.0.push(dp);
                        } else if dp.values.contains_key(&join.right_key) {
                            state.1.push(dp);
                        }
                    }
                    let mut results = Vec::new();
                    for left in &state.0 {
                        for right in &state.1 {
                            if let Ok(joined) = join.apply(left, right) {
                                results.push(joined);
                            }
                        }
                    }
                    // Clean up old data points
                    let cutoff = self.time_manager.get_current_watermark().await - join.window;
                    state.0.retain(|dp| dp.timestamp > cutoff);
                    state.1.retain(|dp| dp.timestamp > cutoff);
                    Ok(results)
                }).await.context("Join operation failed")?
            },
            Operator::Aggregate(aggregate) => {
                let result = aggregate.apply(&data_points)?;
                Ok(vec![DataPoint {
                    id: Uuid::new_v4(),
                    timestamp: self.time_manager.get_current_watermark().await,
                    values: HashMap::from([("result".to_string(), result)]),
                }])
            },
            Operator::FlatMap(flat_map) => {
                let mut results = Vec::new();
                for dp in data_points {
                    results.extend(flat_map.apply(dp)?);
                }
                Ok(results)
            },
            Operator::GroupBy(group_by) => {
                let grouped = group_by.apply(&data_points)?;
                Ok(grouped.into_iter().map(|(key, value)| DataPoint {
                    id: Uuid::new_v4(),
                    timestamp: self.time_manager.get_current_watermark().await,
                    values: HashMap::from([
                        ("key".to_string(), Value::String(key)),
                        ("value".to_string(), value),
                    ]),
                }).collect())
            },
        }
    }

    pub async fn add_pipeline(&self, pipeline: Pipeline) -> Result<(), anyhow::Error> {
        self.pipelines.write().await.insert(pipeline.id, pipeline);
        Ok(())
    }

    async fn set_state_with_error_handling<T: serde::Serialize>(&self, key: &str, value: &T, state_type: StateType) -> Result<(), anyhow::Error> {
        match self.state_manager.put(key, value, state_type).await {
            Ok(_) => Ok(()),
            Err(e) => match e {
                StateError::SerializationError(se) => Err(anyhow::anyhow!("Serialization error: {}", se)),
                StateError::DeserializationError(de) => Err(anyhow::anyhow!("Deserialization error: {}", de)),
                StateError::StorageError(se) => Err(anyhow::anyhow!("Storage error: {}", se)),
                _ => Err(anyhow::anyhow!("State error: {:?}", e)),
            },
        }
    }
}

#[derive(Clone)]
pub struct Pipeline {
    pub id: Uuid,
    pub stages: Vec<PipelineStage>,
}

#[derive(Clone)]
pub struct PipelineStage {
    pub id: Uuid,
    pub operator: Operator,
}