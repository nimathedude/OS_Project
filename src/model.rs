use std::collections::HashMap;

/// Linux CFS nice weights table for nice -20..19 (index = nice + 20)
pub const NICE_TO_WEIGHT: [u32; 40] = [
    88761, 71755, 56483, 46273, 36291,
    29154, 23254, 18705, 14949, 11916,
    9548, 7620, 6100, 4904, 3906,
    3121, 2501, 1991, 1586, 1277,
    1024, 820, 655, 526, 423,
    335, 272, 215, 172, 137,
    110, 87, 70, 56, 45,
    36, 29, 23, 18, 15
];

pub const NICE_0_LOAD: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Runnable,
    Blocked,
    Exited,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub nice: i32,
    pub weight: u32,
    pub vruntime: i64,
    pub state: TaskState,

    /// CPU affinity bitmask. bit i => can run on CPU i
    pub affinity_mask: u64,

    /// cgroup id (None => root)
    pub cgroup: Option<String>,

    /// last cpu it ran on (for migrations accounting)
    pub last_cpu: Option<usize>,

    /// If true: after CPU_BURST, task vruntime is no longer tracked/updated
    pub burst_mode: bool,
}

impl Task {
    pub fn new(id: String, nice: i32, vruntime: i64, affinity_mask: u64, cgroup: Option<String>) -> Self {
        let nice = nice.clamp(-20, 19);
        let weight = NICE_TO_WEIGHT[(nice + 20) as usize];
        Self {
            id,
            nice,
            weight,
            vruntime,
            state: TaskState::Runnable,
            affinity_mask,
            cgroup,
            last_cpu: None,
            burst_mode: false,
        }
    }

    pub fn set_nice(&mut self, nice: i32) {
        self.nice = nice.clamp(-20, 19);
        self.weight = NICE_TO_WEIGHT[(self.nice + 20) as usize];
    }

    pub fn can_run_on(&self, cpu: usize) -> bool {
        if cpu >= 64 { return false; }
        (self.affinity_mask & (1u64 << cpu)) != 0
    }
}

#[derive(Debug, Clone)]
pub struct Cgroup {
    pub id: String,
    pub cpu_shares: u32,
    pub cpu_mask: u64,

    /// group-level virtual runtime (for fairness between cgroups)
    pub vruntime: i64,
}

impl Cgroup {
    pub fn new(id: String) -> Self {
        Self {
            id,
            cpu_shares: 1024,
            cpu_mask: !0u64,
            vruntime: 0,
        }
    }

    pub fn can_run_on(&self, cpu: usize) -> bool {
        if cpu >= 64 { return false; }
        (self.cpu_mask & (1u64 << cpu)) != 0
    }
}

#[derive(Debug, Default)]
pub struct World {
    pub tasks: HashMap<String, Task>,
    pub cgroups: HashMap<String, Cgroup>,
    pub last_schedule: Vec<Option<String>>,
}

/// Root (no-cgroup) pseudo-id
pub const ROOT_CGROUP: &str = "__root__";
