use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use futures::future::BoxFuture;
use serde::{Serialize, Deserialize};
use std::collections::{VecDeque, HashMap, HashSet};

use crate::state_manager::{StateManager, StateType, StateError};
use crate::time_manager::TimeManager;

#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    #[error("Data point filtered out")]
    Filtered,
    #[error("State error: {0}")]
    StateError(#[from] StateError),
    #[error("Transformation error: {0}")]
    TransformError(String),
    #[error("Aggregation error: {0}")]
    AggregationError(String),
    #[error("Window incomplete")]
    WindowIncomplete,
    #[error("Join error: {0}")]
    JoinError(String),
    #[error("Out of order event: {0}")]
    OutOfOrderEvent(String),
    #[error("Complex event processing error: {0}")]
    CEPError(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataPoint {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub stream_id: Uuid,
    pub values: HashMap<String, serde_json::Value>,
}

pub trait Operator: Send + Sync {
    fn process(&self, input: DataPoint, state_manager: &StateManager, time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>>;
}

// Enhanced Filter Operator
pub struct FilterOperator {
    predicate: Arc<dyn Fn(&DataPoint) -> bool + Send + Sync>,
}

impl FilterOperator {
    pub fn new<F>(predicate: F) -> Self
    where
        F: Fn(&DataPoint) -> bool + Send + Sync + 'static,
    {
        Self { predicate: Arc::new(predicate) }
    }
}

impl Operator for FilterOperator {
    fn process(&self, input: DataPoint, _state_manager: &StateManager, _time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let predicate = self.predicate.clone();
        Box::pin(async move {
            if predicate(&input) {
                Ok(vec![input])
            } else {
                Ok(vec![])
            }
        })
    }
}

// Enhanced Map Operator
pub struct MapOperator {
    transform: Arc<dyn Fn(DataPoint) -> Result<DataPoint, OperatorError> + Send + Sync>,
}

impl MapOperator {
    pub fn new<F>(transform: F) -> Self
    where
        F: Fn(DataPoint) -> Result<DataPoint, OperatorError> + Send + Sync + 'static,
    {
        Self { transform: Arc::new(transform) }
    }
}

impl Operator for MapOperator {
    fn process(&self, input: DataPoint, _state_manager: &StateManager, _time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let transform = self.transform.clone();
        Box::pin(async move {
            transform(input).map(|dp| vec![dp])
        })
    }
}

// Enhanced Window Operator
pub struct WindowOperator {
    window_type: WindowType,
    stream_id: String,
    aggregate_fn: Arc<dyn Fn(Vec<DataPoint>) -> Result<DataPoint, OperatorError> + Send + Sync>,
}

#[derive(Clone, Debug)]
pub enum WindowType {
    Tumbling(Duration),
    Sliding { window: Duration, slide: Duration },
    Session { gap: Duration },
}

impl Operator for WindowOperator {
    fn process(&self, input: DataPoint, state_manager: &StateManager, time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let stream_id = self.stream_id.clone();
        let window_type = self.window_type.clone();
        let aggregate_fn = self.aggregate_fn.clone();

        Box::pin(async move {
            let key = format!("window:{}", stream_id);
            let mut result = Vec::new();

            match window_type {
                WindowType::Tumbling(size) => {
                    let window_end = input.timestamp.truncate(size) + size;
                    state_manager.update(&key, StateType::Local, |window: &mut Vec<DataPoint>| {
                        window.push(input.clone());
                        if input.timestamp >= window_end {
                            let to_aggregate = std::mem::take(window);
                            if let Ok(aggregated) = aggregate_fn(to_aggregate) {
                                result.push(aggregated);
                            }
                        }
                        Ok(())
                    }).await?;
                },
                WindowType::Sliding { window, slide } => {
                    let window_end = input.timestamp.truncate(slide) + window;
                    state_manager.update(&key, StateType::Local, |windows: &mut VecDeque<(DateTime<Utc>, Vec<DataPoint>)>| {
                        windows.push_back((window_end, vec![input.clone()]));
                        while let Some((end, mut window)) = windows.pop_front() {
                            if end <= input.timestamp - window {
                                if let Ok(aggregated) = aggregate_fn(window) {
                                    result.push(aggregated);
                                }
                            } else {
                                windows.push_front((end, window));
                                break;
                            }
                        }
                        Ok(())
                    }).await?;
                },
                WindowType::Session { gap } => {
                    state_manager.update(&key, StateType::Local, |sessions: &mut Vec<(DateTime<Utc>, Vec<DataPoint>)>| {
                        if let Some((last_time, session)) = sessions.last_mut() {
                            if input.timestamp - *last_time <= gap {
                                session.push(input.clone());
                                *last_time = input.timestamp;
                            } else {
                                if let Ok(aggregated) = aggregate_fn(std::mem::take(session)) {
                                    result.push(aggregated);
                                }
                                sessions.push((input.timestamp, vec![input.clone()]));
                            }
                        } else {
                            sessions.push((input.timestamp, vec![input.clone()]));
                        }
                        Ok(())
                    }).await?;
                },
            }

            time_manager.update_watermark(input.timestamp).await;

            Ok(result)
        })
    }
}

// Enhanced Join Operator
#[derive(Clone, Serialize, Deserialize)]
struct JoinState {
    left_stream: VecDeque<DataPoint>,
    right_stream: VecDeque<DataPoint>,
}

pub struct JoinOperator {
    left_stream_id: Uuid,
    right_stream_id: Uuid,
    join_window: Duration,
    join_type: JoinType,
    join_fn: Arc<dyn Fn(&DataPoint, &DataPoint) -> Result<DataPoint, OperatorError> + Send + Sync>,
}

#[derive(Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl JoinOperator {
    pub fn new<F>(left_stream_id: Uuid, right_stream_id: Uuid, join_window: Duration, join_type: JoinType, join_fn: F) -> Self
    where
        F: Fn(&DataPoint, &DataPoint) -> Result<DataPoint, OperatorError> + Send + Sync + 'static,
    {
        Self {
            left_stream_id,
            right_stream_id,
            join_window,
            join_type,
            join_fn: Arc::new(join_fn),
        }
    }
}

impl Operator for JoinOperator {
    fn process(&self, input: DataPoint, state_manager: &StateManager, time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let join_fn = self.join_fn.clone();
        let join_window = self.join_window;
        let join_type = self.join_type;

        Box::pin(async move {
            let key = format!("join:{}:{}", self.left_stream_id, self.right_stream_id);
            let mut joined_data = Vec::new();

            state_manager.update(&key, StateType::Local, |join_state: &mut JoinState| {
                let (current_stream, other_stream) = if input.stream_id == self.left_stream_id {
                    (&mut join_state.left_stream, &join_state.right_stream)
                } else {
                    (&mut join_state.right_stream, &join_state.left_stream)
                };

                current_stream.push_back(input.clone());

                while let Some(front) = current_stream.front() {
                    if input.timestamp - front.timestamp > join_window {
                        current_stream.pop_front();
                    } else {
                        break;
                    }
                }

                for other_dp in other_stream.iter() {
                    if (input.timestamp - other_dp.timestamp).abs() <= join_window {
                        match join_fn(&input, other_dp) {
                            Ok(joined) => joined_data.push(joined),
                            Err(e) => eprintln!("Join error: {}", e),
                        }
                    }
                }

                match join_type {
                    JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                        if joined_data.is_empty() {
                            joined_data.push(input.clone());
                        }
                    },
                    _ => {}
                }

                Ok(())
            }).await.map_err(|e| OperatorError::StateError(e))?;

            time_manager.update_watermark(input.timestamp).await;

            Ok(joined_data)
        })
    }
}

// New Complex Event Processing Operator
pub struct CEPOperator {
    pattern: Arc<dyn Fn(&[DataPoint]) -> bool + Send + Sync>,
    max_pattern_length: usize,
    output_transformer: Arc<dyn Fn(&[DataPoint]) -> Result<DataPoint, OperatorError> + Send + Sync>,
}

impl CEPOperator {
    pub fn new<P, T>(pattern: P, max_pattern_length: usize, output_transformer: T) -> Self
    where
        P: Fn(&[DataPoint]) -> bool + Send + Sync + 'static,
        T: Fn(&[DataPoint]) -> Result<DataPoint, OperatorError> + Send + Sync + 'static,
    {
        Self {
            pattern: Arc::new(pattern),
            max_pattern_length,
            output_transformer: Arc::new(output_transformer),
        }
    }
}

impl Operator for CEPOperator {
    fn process(&self, input: DataPoint, state_manager: &StateManager, time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let pattern = self.pattern.clone();
        let output_transformer = self.output_transformer.clone();
        let max_pattern_length = self.max_pattern_length;

        Box::pin(async move {
            let key = format!("cep:{}", input.stream_id);
            let mut result = Vec::new();

            state_manager.update(&key, StateType::Local, |buffer: &mut VecDeque<DataPoint>| {
                buffer.push_back(input.clone());
                if buffer.len() > max_pattern_length {
                    buffer.pop_front();
                }

                for i in 1..=buffer.len() {
                    let window: Vec<_> = buffer.iter().rev().take(i).rev().cloned().collect();
                    if pattern(&window) {
                        match output_transformer(&window) {
                            Ok(output) => result.push(output),
                            Err(e) => return Err(OperatorError::CEPError(e.to_string())),
                        }
                    }
                }

                Ok(())
            }).await?;

            time_manager.update_watermark(input.timestamp).await;

            Ok(result)
        })
    }
}

// New Shuffle Operator
pub struct ShuffleOperator {
    partition_fn: Arc<dyn Fn(&DataPoint) -> u64 + Send + Sync>,
    num_partitions: u64,
}

impl ShuffleOperator {
    pub fn new<F>(partition_fn: F, num_partitions: u64) -> Self
    where
        F: Fn(&DataPoint) -> u64 + Send + Sync + 'static,
    {
        Self {
            partition_fn: Arc::new(partition_fn),
            num_partitions,
        }
    }
}

impl Operator for ShuffleOperator {
    fn process(&self, input: DataPoint, _state_manager: &StateManager, _time_manager: &TimeManager) -> BoxFuture<'_, Result<Vec<DataPoint>, OperatorError>> {
        let partition_fn = self.partition_fn.clone();
        let num_partitions = self.num_partitions;

        Box::pin(async move {
            let partition = partition_fn(&input) % num_partitions;
            let mut shuffled = input;
            shuffled.values.insert("__partition".to_string(), serde_json::Value::Number(partition.into()));
            Ok(vec![shuffled])
        })
    }
}