use anyhow::{anyhow, Result};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::model::{Cgroup, Task, TaskState, World, NICE_0_LOAD, ROOT_CGROUP};

#[derive(Debug, Clone)]
struct TaskHeapItem {
    vruntime: i64,
    task_id: String,
    seq: u64,
}
impl PartialEq for TaskHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.vruntime == other.vruntime && self.task_id == other.task_id && self.seq == other.seq
    }
}
impl Eq for TaskHeapItem {}
impl PartialOrd for TaskHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TaskHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // min-heap by vruntime
        other
            .vruntime
            .cmp(&self.vruntime)
            .then_with(|| other.seq.cmp(&self.seq))
            .then_with(|| other.task_id.cmp(&self.task_id))
    }
}

#[derive(Debug, Clone)]
struct CgHeapItem {
    vruntime: i64,
    cgid: String,
    seq: u64,
}
impl PartialEq for CgHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.vruntime == other.vruntime && self.cgid == other.cgid && self.seq == other.seq
    }
}
impl Eq for CgHeapItem {}
impl PartialOrd for CgHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CgHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // min-heap by vruntime
        other
            .vruntime
            .cmp(&self.vruntime)
            .then_with(|| other.seq.cmp(&self.seq))
            .then_with(|| other.cgid.cmp(&self.cgid))
    }
}

#[derive(Debug)]
pub struct TickResult {
    pub vtime: i64,
    pub schedule: Vec<String>,
    pub preemptions: u64,
    pub migrations: u64,
    pub runnable: Vec<String>,
    pub blocked: Vec<String>,
}

pub struct Scheduler {
    cpus: usize,
    quanta: i64,

    cg_heap: BinaryHeap<CgHeapItem>,
    cg_seq: u64,
    cg_last_key: HashMap<String, i64>,

    rq: HashMap<String, BinaryHeap<TaskHeapItem>>,
    task_seq: u64,
    task_last_key: HashMap<String, i64>,
}

impl Scheduler {
    pub fn new(cpus: usize, quanta: i64) -> Self {
        Self {
            cpus,
            quanta,
            cg_heap: BinaryHeap::new(),
            cg_seq: 0,
            cg_last_key: HashMap::new(),
            rq: HashMap::new(),
            task_seq: 0,
            task_last_key: HashMap::new(),
        }
    }

    fn vruntime_delta_task(&self, weight: u32) -> i64 {
        ((self.quanta as i128 * NICE_0_LOAD as i128) / weight as i128) as i64
    }

    fn vruntime_delta_cg(&self, shares: u32) -> i64 {
        let shares = shares.max(1);
        ((self.quanta as i128 * NICE_0_LOAD as i128) / shares as i128) as i64
    }

    fn ensure_root_cgroup(world: &mut World) {
        if !world.cgroups.contains_key(ROOT_CGROUP) {
            world.cgroups
                .insert(ROOT_CGROUP.to_string(), Cgroup::new(ROOT_CGROUP.to_string()));
        }
    }

    fn task_cgid(t: &Task) -> String {
        t.cgroup.clone().unwrap_or_else(|| ROOT_CGROUP.to_string())
    }

    fn push_cgroup(&mut self, world: &World, cgid: &str) {
        if !world.cgroups.contains_key(cgid) {
            return;
        }
        let cg = world.cgroups.get(cgid).unwrap();
        self.cg_seq += 1;
        self.cg_heap.push(CgHeapItem {
            vruntime: cg.vruntime,
            cgid: cgid.to_string(),
            seq: self.cg_seq,
        });
        self.cg_last_key.insert(cgid.to_string(), cg.vruntime);
    }

    fn push_task_into_cg(&mut self, world: &World, task_id: &str) {
        let Some(t) = world.tasks.get(task_id) else { return; };
        if t.state != TaskState::Runnable {
            return;
        }

        let cgid = Self::task_cgid(t);
        let heap = self.rq.entry(cgid.clone()).or_insert_with(BinaryHeap::new);

        self.task_seq += 1;
        heap.push(TaskHeapItem {
            vruntime: t.vruntime,
            task_id: task_id.to_string(),
            seq: self.task_seq,
        });
        self.task_last_key.insert(task_id.to_string(), t.vruntime);

        self.push_cgroup(world, &cgid);
    }

    fn is_cg_item_valid(&self, world: &World, it: &CgHeapItem) -> bool {
        let Some(cg) = world.cgroups.get(&it.cgid) else { return false; };
        if cg.vruntime != it.vruntime {
            return false;
        }
        self.cg_last_key
            .get(&it.cgid)
            .copied()
            .unwrap_or(it.vruntime)
            == it.vruntime
    }

    // ✅ این یکی دیگر &self نمی‌خواهد؛ فقط از snapshot map استفاده می‌کند
    fn is_task_item_valid_snapshot(
        world: &World,
        task_last_key_snapshot: &HashMap<String, i64>,
        it: &TaskHeapItem,
    ) -> bool {
        let Some(t) = world.tasks.get(&it.task_id) else { return false; };
        if t.state != TaskState::Runnable {
            return false;
        }
        if t.vruntime != it.vruntime {
            return false;
        }
        task_last_key_snapshot
            .get(&it.task_id)
            .copied()
            .unwrap_or(it.vruntime)
            == it.vruntime
    }

    fn pick_for_cpu(&mut self, world: &World, cpu: usize, chosen: &HashSet<String>) -> Option<String> {
        // ✅ snapshot برای جلوگیری از borrow conflict
        let task_last_key_snapshot = self.task_last_key.clone();

        let mut skipped_cg: Vec<CgHeapItem> = Vec::new();
        let mut picked: Option<String> = None;

        while let Some(cgit) = self.cg_heap.pop() {
            if !self.is_cg_item_valid(world, &cgit) {
                continue;
            }

            let cg = world.cgroups.get(&cgit.cgid).unwrap();
            if !cg.can_run_on(cpu) {
                skipped_cg.push(cgit);
                continue;
            }

            let Some(heap) = self.rq.get_mut(&cgit.cgid) else {
                skipped_cg.push(cgit);
                continue;
            };

            let mut skipped_tasks: Vec<TaskHeapItem> = Vec::new();
            let mut found: Option<String> = None;

            while let Some(tit) = heap.pop() {
                if !Self::is_task_item_valid_snapshot(world, &task_last_key_snapshot, &tit) {
                    continue;
                }
                if chosen.contains(&tit.task_id) {
                    skipped_tasks.push(tit);
                    continue;
                }

                let t = world.tasks.get(&tit.task_id).unwrap();
                if !t.can_run_on(cpu) {
                    skipped_tasks.push(tit);
                    continue;
                }

                found = Some(tit.task_id.clone());
                break;
            }

            for it in skipped_tasks {
                heap.push(it);
            }

            if let Some(tid) = found {
                picked = Some(tid);
                skipped_cg.push(cgit);
                break;
            } else {
                skipped_cg.push(cgit);
            }
        }

        for it in skipped_cg {
            self.cg_heap.push(it);
        }

        picked
    }

    fn min_vruntime(world: &World) -> i64 {
        world.tasks
            .values()
            .filter(|t| t.state == TaskState::Runnable)
            .map(|t| t.vruntime)
            .min()
            .unwrap_or(0)
    }

    fn max_vruntime(world: &World) -> i64 {
        world.tasks
            .values()
            .filter(|t| t.state == TaskState::Runnable)
            .map(|t| t.vruntime)
            .max()
            .unwrap_or(0)
    }

    pub fn on_tick(&mut self, world: &mut World, vtime: i64, events: &[Value]) -> Result<TickResult> {
        Self::ensure_root_cgroup(world);

        for ev in events {
            self.apply_event(world, ev)?;
        }

        let mut schedule: Vec<String> = Vec::with_capacity(self.cpus);
        let mut chosen: HashSet<String> = HashSet::new();

        for cpu in 0..self.cpus {
            if let Some(tid) = self.pick_for_cpu(world, cpu, &chosen) {
                chosen.insert(tid.clone());
                schedule.push(tid);
            } else {
                schedule.push("idle".to_string());
            }
        }

        if world.last_schedule.len() != self.cpus {
            world.last_schedule = vec![None; self.cpus];
        }

        let mut preemptions: u64 = 0;
        let mut migrations: u64 = 0;

        for cpu in 0..self.cpus {
            let prev = world.last_schedule[cpu].clone();
            let now = schedule[cpu].clone();

            if now != "idle" {
                if let Some(p) = prev.clone() {
                    if p != now {
                        preemptions += 1;
                    }
                }
                if let Some(t) = world.tasks.get_mut(&now) {
                    if let Some(last) = t.last_cpu {
                        if last != cpu {
                            migrations += 1;
                        }
                    }
                    t.last_cpu = Some(cpu);
                }
                world.last_schedule[cpu] = Some(now);
            } else {
                if prev.is_some() {
                    preemptions += 1;
                }
                world.last_schedule[cpu] = None;
            }
        }

        for cpu in 0..self.cpus {
            let tid = &schedule[cpu];
            if tid == "idle" {
                continue;
            }

            let (cgid, weight, burst) = {
                let t = world.tasks.get(tid).unwrap();
                (Self::task_cgid(t), t.weight, t.burst_mode)
            };

            if let Some(t) = world.tasks.get_mut(tid) {
                if t.state == TaskState::Runnable && !burst {
                    t.vruntime = t.vruntime.saturating_add(self.vruntime_delta_task(weight));
                    self.task_last_key.insert(tid.clone(), t.vruntime);
                }
            }

            if let Some(cg) = world.cgroups.get_mut(&cgid) {
                cg.vruntime = cg.vruntime.saturating_add(self.vruntime_delta_cg(cg.cpu_shares));
                self.cg_last_key.insert(cgid.clone(), cg.vruntime);
            }

            self.push_task_into_cg(world, tid);
            self.push_cgroup(world, &cgid);
        }

        let mut runnable: Vec<String> = Vec::new();
        let mut blocked: Vec<String> = Vec::new();
        for (id, t) in world.tasks.iter() {
            match t.state {
                TaskState::Runnable => runnable.push(id.clone()),
                TaskState::Blocked => blocked.push(id.clone()),
                TaskState::Exited => {}
            }
        }
        runnable.sort();
        blocked.sort();

        Ok(TickResult {
            vtime,
            schedule,
            preemptions,
            migrations,
            runnable,
            blocked,
        })
    }

    fn get_action<'a>(obj: &'a serde_json::Map<String, Value>) -> Option<&'a str> {
        obj.get("action")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("type").and_then(|v| v.as_str()))
            .or_else(|| obj.get("event").and_then(|v| v.as_str()))
    }

    fn get_task_id<'a>(obj: &'a serde_json::Map<String, Value>) -> Option<&'a str> {
        obj.get("taskId")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("id").and_then(|v| v.as_str()))
    }

    fn get_cgroup_id<'a>(obj: &'a serde_json::Map<String, Value>) -> Option<&'a str> {
        obj.get("cgroupId")
            .and_then(|v| v.as_str())
            .or_else(|| obj.get("cgroup").and_then(|v| v.as_str()))
    }

    fn parse_cpu_mask_array(mask: &[Value]) -> Result<u64> {
        let mut bits: u64 = 0;
        for v in mask {
            let idx = v
                .as_u64()
                .ok_or_else(|| anyhow!("cpuMask entry must be int"))?;
            if idx >= 64 {
                return Err(anyhow!("cpu index out of range: {}", idx));
            }
            bits |= 1u64 << idx;
        }
        Ok(bits)
    }

    fn get_cpu_mask(obj: &serde_json::Map<String, Value>, array_key: &str, legacy_key: &str) -> Result<Option<u64>> {
        if let Some(arr) = obj.get(array_key).and_then(|v| v.as_array()) {
            return Ok(Some(Self::parse_cpu_mask_array(arr)?));
        }
        if let Some(mask) = obj.get(legacy_key).and_then(|v| v.as_u64()) {
            return Ok(Some(mask));
        }
        Ok(None)
    }

    fn apply_event(&mut self, world: &mut World, ev: &Value) -> Result<()> {
        let obj = ev.as_object().ok_or_else(|| anyhow!("event not object"))?;
        let typ = Self::get_action(obj).unwrap_or("UNKNOWN");

        match typ {
            "TASK_CREATE" => {
                let id = Self::get_task_id(obj)
                    .ok_or_else(|| anyhow!("TASK_CREATE missing taskId"))?
                    .to_string();
                let nice = obj.get("nice").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let vr = obj.get("vruntime").and_then(|v| v.as_i64()).unwrap_or(0);
                let aff = Self::get_cpu_mask(obj, "cpuMask", "affinity")?.unwrap_or(!0u64);
                let cgroup = Self::get_cgroup_id(obj).map(|s| s.to_string());

                let t = Task::new(id.clone(), nice, vr, aff, cgroup);
                world.tasks.insert(id.clone(), t);
                self.push_task_into_cg(world, &id);
            }

            "TASK_EXIT" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_EXIT missing taskId"))?;
                if let Some(t) = world.tasks.get_mut(id) {
                    t.state = TaskState::Exited;
                }
            }

            "TASK_BLOCK" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_BLOCK missing taskId"))?;
                if let Some(t) = world.tasks.get_mut(id) {
                    t.state = TaskState::Blocked;
                }
            }

            "TASK_UNBLOCK" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_UNBLOCK missing taskId"))?;
                let min_vr = Self::min_vruntime(world);
                if let Some(t) = world.tasks.get_mut(id) {
                    t.state = TaskState::Runnable;
                    if t.vruntime < min_vr {
                        t.vruntime = min_vr;
                    }
                }
                self.push_task_into_cg(world, id);
            }

            "TASK_YIELD" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_YIELD missing taskId"))?;
                let max_vr = Self::max_vruntime(world);
                if let Some(t) = world.tasks.get_mut(id) {
                    t.vruntime = max_vr;
                }
                self.push_task_into_cg(world, id);
            }

            "TASK_SETNICE" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_SETNICE missing taskId"))?;
                let nice = obj
                    .get("nice")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| anyhow!("TASK_SETNICE missing nice"))? as i32;
                if let Some(t) = world.tasks.get_mut(id) {
                    t.set_nice(nice);
                }
                self.push_task_into_cg(world, id);
            }

            "TASK_SET_AFFINITY" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_SET_AFFINITY missing taskId"))?;
                let mask = Self::get_cpu_mask(obj, "cpuMask", "affinity")?
                    .ok_or_else(|| anyhow!("TASK_SET_AFFINITY missing cpuMask"))?;
                if let Some(t) = world.tasks.get_mut(id) {
                    t.affinity_mask = mask;
                }
                self.push_task_into_cg(world, id);
            }

            "TASK_MOVE_CGROUP" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("TASK_MOVE_CGROUP missing taskId"))?;
                let cg = obj
                    .get("newCgroupId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| Self::get_cgroup_id(obj).map(|s| s.to_string()));
                if let Some(t) = world.tasks.get_mut(id) {
                    t.cgroup = cg;
                }
                self.push_task_into_cg(world, id);
            }

            "CGROUP_CREATE" => {
                let id = Self::get_cgroup_id(obj)
                    .ok_or_else(|| anyhow!("CGROUP_CREATE missing cgroupId"))?
                    .to_string();
                let mut cg = Cgroup::new(id.clone());
                if let Some(sh) = obj
                    .get("cpuShares")
                    .and_then(|v| v.as_i64())
                    .or_else(|| obj.get("cpu_shares").and_then(|v| v.as_i64()))
                {
                    cg.cpu_shares = (sh as i64).max(1) as u32;
                }
                if let Some(mask) = Self::get_cpu_mask(obj, "cpuMask", "cpu_mask")? {
                    cg.cpu_mask = mask;
                }
                let _cpu_quota = obj.get("cpuQuotaUs").and_then(|v| v.as_i64());
                let _cpu_period = obj.get("cpuPeriodUs").and_then(|v| v.as_i64());
                world.cgroups.insert(id.clone(), cg);
                self.push_cgroup(world, &id);
            }

            "CGROUP_MODIFY" => {
                let id = Self::get_cgroup_id(obj)
                    .ok_or_else(|| anyhow!("CGROUP_MODIFY missing cgroupId"))?
                    .to_string();
                let cg = world.cgroups.entry(id.clone()).or_insert_with(|| Cgroup::new(id.clone()));
                if let Some(sh) = obj
                    .get("cpuShares")
                    .and_then(|v| v.as_i64())
                    .or_else(|| obj.get("cpu_shares").and_then(|v| v.as_i64()))
                {
                    cg.cpu_shares = (sh as i64).max(1) as u32;
                }
                if let Some(mask) = Self::get_cpu_mask(obj, "cpuMask", "cpu_mask")? {
                    cg.cpu_mask = mask;
                }
                let _cpu_quota = obj.get("cpuQuotaUs").and_then(|v| v.as_i64());
                let _cpu_period = obj.get("cpuPeriodUs").and_then(|v| v.as_i64());
                self.push_cgroup(world, &id);
            }

            "CGROUP_DELETE" => {
                let id = Self::get_cgroup_id(obj).ok_or_else(|| anyhow!("CGROUP_DELETE missing cgroupId"))?;
                world.cgroups.remove(id);
                for t in world.tasks.values_mut() {
                    if t.cgroup.as_deref() == Some(id) {
                        t.cgroup = None;
                    }
                }
            }

            "CPU_BURST" => {
                let id = Self::get_task_id(obj).ok_or_else(|| anyhow!("CPU_BURST missing taskId"))?;
                if let Some(t) = world.tasks.get_mut(id) {
                    t.burst_mode = true;
                }
                self.push_task_into_cg(world, id);
            }

            _ => {}
        }

        Ok(())
    }
}
