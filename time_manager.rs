use std::collections::BTreeMap;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;
use futures::future::BoxFuture;

pub struct TimeManager {
    watermark: Arc<RwLock<Watermark>>,
    timers: Arc<RwLock<BTreeMap<DateTime<Utc>, Vec<Timer>>>>,
    max_out_of_orderness: Duration,
}

struct Watermark {
    current: DateTime<Utc>,
}

struct Timer {
    id: String,
    callback: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
}

impl TimeManager {
    pub fn new(max_out_of_orderness: Duration) -> Self {
        Self {
            watermark: Arc::new(RwLock::new(Watermark { current: DateTime::MIN_UTC })),
            timers: Arc::new(RwLock::new(BTreeMap::new())),
            max_out_of_orderness,
        }
    }

    pub async fn update_watermark(&self, event_time: DateTime<Utc>) {
        let mut watermark = self.watermark.write().await;
        let new_watermark = event_time - self.max_out_of_orderness;
        if new_watermark > watermark.current {
            watermark.current = new_watermark;
            drop(watermark);
            self.trigger_timers(new_watermark).await;
        }
    }

    pub async fn get_current_watermark(&self) -> DateTime<Utc> {
        self.watermark.read().await.current
    }

    pub async fn register_timer(&self, time: DateTime<Utc>, id: String, callback: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static) {
        let timer = Timer {
            id,
            callback: Arc::new(callback),
        };
        let mut timers = self.timers.write().await;
        timers.entry(time).or_insert_with(Vec::new).push(timer);
    }

    pub async fn is_window_complete(&self, window_end: DateTime<Utc>) -> bool {
        self.get_current_watermark().await >= window_end
    }

    pub async fn handle_late_event(&self, event_time: DateTime<Utc>) -> bool {
        let watermark = self.get_current_watermark().await;
        event_time >= watermark - self.max_out_of_orderness
    }

    async fn trigger_timers(&self, up_to: DateTime<Utc>) {
        let mut timers = self.timers.write().await;
        let mut triggered_timers = Vec::new();

        // Collect timers to trigger
        for (&time, timer_list) in timers.range(..=up_to) {
            triggered_timers.extend(timer_list.iter().cloned());
        }

        // Remove triggered timers
        timers.retain(|&time, _| time > up_to);

        // Drop the lock before executing callbacks
        drop(timers);

        // Execute callbacks
        for timer in triggered_timers {
            let callback = timer.callback.clone();
            tokio::spawn(async move {
                callback().await;
            });
        }
    }

    pub async fn get_current_event_time(&self) -> DateTime<Utc> {
        self.watermark.read().await.current + self.max_out_of_orderness
    }

    pub fn get_processing_time(&self) -> DateTime<Utc> {
        Utc::now()
    }

    pub async fn cleanup_timers(&self, before: DateTime<Utc>) {
        let mut timers = self.timers.write().await;
        timers.retain(|&time, _| time >= before);
    }
}