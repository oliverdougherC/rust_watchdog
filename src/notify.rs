use crate::config::NotifyConfig;
use serde::Serialize;
use serde_json::json;
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::OnceLock;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyEvent {
    PassFailureSummary,
    ReplacementSummary,
    CooldownAlert,
}

impl NotifyEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PassFailureSummary => "pass_failure_summary",
            Self::ReplacementSummary => "replacement_summary",
            Self::CooldownAlert => "cooldown_alert",
        }
    }
}

fn enabled(config: &NotifyConfig, event: NotifyEvent) -> bool {
    !config.webhook_url.is_empty() && config.events.iter().any(|e| e == event.as_str())
}

#[derive(Debug)]
struct NotifyMessage {
    url: String,
    event: String,
    timeout_seconds: u64,
    body: String,
}

static NOTIFY_TX: OnceLock<SyncSender<NotifyMessage>> = OnceLock::new();

fn notify_sender() -> &'static SyncSender<NotifyMessage> {
    NOTIFY_TX.get_or_init(|| {
        let (tx, rx) = sync_channel::<NotifyMessage>(256);
        if let Err(e) = std::thread::Builder::new()
            .name("watchdog-notify-worker".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    let response = minreq::post(&msg.url)
                        .with_timeout(msg.timeout_seconds)
                        .with_header("content-type", "application/json")
                        .with_body(msg.body)
                        .send();
                    match response {
                        Ok(resp) => {
                            if resp.status_code >= 400 {
                                warn!(
                                    "Webhook for '{}' returned HTTP {}",
                                    msg.event, resp.status_code
                                );
                            }
                        }
                        Err(e) => warn!("Webhook delivery failed for '{}': {}", msg.event, e),
                    }
                }
            })
        {
            warn!(
                "Failed to spawn notify worker thread (notifications disabled): {}",
                e
            );
        }
        tx
    })
}

pub fn send_webhook<T: Serialize>(config: &NotifyConfig, event: NotifyEvent, payload: &T) {
    if !enabled(config, event) {
        return;
    }

    let body = json!({
        "event": event.as_str(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "payload": payload,
    });

    let msg = NotifyMessage {
        url: config.webhook_url.clone(),
        event: event.as_str().to_string(),
        timeout_seconds: config.timeout_seconds,
        body: body.to_string(),
    };
    match notify_sender().try_send(msg) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            warn!(
                "Notify queue full; dropping webhook event '{}'",
                event.as_str()
            );
        }
        Err(TrySendError::Disconnected(_)) => {
            warn!(
                "Notify worker disconnected; dropping webhook event '{}'",
                event.as_str()
            );
        }
    }
}
