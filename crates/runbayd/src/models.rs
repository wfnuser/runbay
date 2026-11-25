use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityTier {
    OneShot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdapterCapabilities {
    pub runtime: String,
    pub tier: CapabilityTier,
    pub supports_streaming: bool,
    pub supports_sandbox: bool,
    pub supports_gateway: bool,
    pub supports_native_resume: bool,
    pub supports_hot_process: bool,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary_path: Option<String>,
    pub notes: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterAgentRequest {
    pub agent_id: String,
    pub runtime: String,
    pub workspace_dir: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: String,
    pub runtime: String,
    pub workspace_dir: String,
    pub home_dir: String,
    pub mvcc_dir: String,
    pub config: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStatusView {
    pub agent_id: String,
    pub runtime: String,
    pub workspace_dir: String,
    pub home_dir: String,
    pub config: serde_json::Value,
    pub capabilities: AdapterCapabilities,
    pub workspace_rev: u64,
    pub active_leases: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_run_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunState {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunState::Queued => "queued",
            RunState::Running => "running",
            RunState::Succeeded => "succeeded",
            RunState::Failed => "failed",
            RunState::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "queued" => RunState::Queued,
            "running" => RunState::Running,
            "succeeded" => RunState::Succeeded,
            "cancelled" => RunState::Cancelled,
            _ => RunState::Failed,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitRunRequest {
    pub prompt: String,
    #[serde(default)]
    pub resource_limits: Option<runbay_sandbox::SandboxLimits>,
    #[serde(default = "default_priority")]
    pub priority: String,
    #[serde(default)]
    pub required_rev: Option<u64>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub allowed_model: Option<String>,
    #[serde(default)]
    pub sandbox_mode: Option<String>,
}

fn default_priority() -> String {
    "interactive".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub agent_id: String,
    pub runtime: String,
    pub prompt: String,
    pub state: RunState,
    pub priority: String,
    pub capability_tier: CapabilityTier,
    pub workspace_rev: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_workspace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub sandbox_enforced: bool,
    pub network_isolated: bool,
    pub secretless_gateway: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_transport: Option<String>,
    pub events_count: u64,
    pub events_dropped: u64,
    pub late_events_rejected: u64,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub seq: u64,
    pub run_id: String,
    pub agent_id: String,
    pub event_type: String,
    pub timestamp: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusResponse {
    pub service: String,
    pub version: String,
    pub platform: String,
    pub sandbox_mechanism: String,
    pub sandbox_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_resource_limits: Option<runbay_sandbox::SandboxLimits>,
    pub gateway_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_transport: Option<String>,
    pub max_concurrent_runs: usize,
    pub max_queued_runs: usize,
    pub active_runs: usize,
    pub queued_runs: usize,
    pub registered_agents: usize,
    pub components: Vec<String>,
    pub adapters: BTreeMap<String, AdapterCapabilities>,
    pub restart_semantics: String,
}
