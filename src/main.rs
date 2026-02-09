use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

mod model;
mod scheduler;

use model::World;
use scheduler::{Scheduler, TickResult};

#[derive(Debug, Deserialize)]
struct TimeFrame {
    vtime: i64,
    events: Vec<Value>,
}

#[derive(Debug)]
struct Config {
    cpus: usize,
    quanta: i64,
    socket_path: PathBuf,
    use_stdin: bool,
}

fn parse_args() -> Result<Config> {
    let mut cpus: Option<usize> = None;
    let mut quanta: Option<i64> = None;
    let mut socket: Option<PathBuf> = None;
    let mut use_stdin = false;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--cpus" => cpus = Some(args.next().ok_or_else(|| anyhow!("--cpus needs value"))?.parse()?),
            "--quanta" => quanta = Some(args.next().ok_or_else(|| anyhow!("--quanta needs value"))?.parse()?),
            "--socket" => socket = Some(PathBuf::from(args.next().ok_or_else(|| anyhow!("--socket needs value"))?)),
            "--stdin" => use_stdin = true,
            _ => return Err(anyhow!("unknown arg: {}", a)),
        }
    }

    let cpus = cpus.ok_or_else(|| anyhow!("missing --cpus"))?;
    let quanta = quanta.ok_or_else(|| anyhow!("missing --quanta"))?;
    let socket_path = socket.unwrap_or_else(|| PathBuf::from("socket.event"));

    Ok(Config { cpus, quanta, socket_path, use_stdin })
}

fn read_one_json(stream: &mut UnixStream, buf: &mut Vec<u8>) -> Result<Option<TimeFrame>> {
    loop {
        if !buf.is_empty() {
            match serde_json::from_slice::<TimeFrame>(&buf) {
                Ok(tf) => {
                    buf.clear();
                    return Ok(Some(tf));
                }
                Err(e) => {
                    let msg = e.to_string();
                    if !msg.contains("EOF") {
                        if let Some(pos) = buf.iter().position(|&c| c == b'\n') {
                            let line = buf.drain(..=pos).collect::<Vec<u8>>();
                            let line_trim = line.into_iter().filter(|&c| c != b'\n' && c != b'\r').collect::<Vec<u8>>();
                            if line_trim.is_empty() { continue; }
                            let tf: TimeFrame = serde_json::from_slice(&line_trim)
                                .with_context(|| format!("failed parsing JSON line: {}", String::from_utf8_lossy(&line_trim)))?;
                            return Ok(Some(tf));
                        }
                    }
                }
            }
        }

        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            } else {
                let tf: TimeFrame = serde_json::from_slice(buf)?;
                buf.clear();
                return Ok(Some(tf));
            }
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn print_tick(stdout: &mut std::io::Stdout, tr: TickResult) -> Result<()> {
    let out = serde_json::json!({
        "vtime": tr.vtime,
        "schedule": tr.schedule,
        "meta": {
            "preemptions": tr.preemptions,
            "migrations": tr.migrations,
            "runnableTasks": tr.runnable,
            "blockedTasks": tr.blocked
        }
    });
    writeln!(stdout, "{}", out.to_string())?;
    stdout.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let cfg = parse_args()?;
    eprintln!(
        "ALFS starting: cpus={}, quanta={}, socket={}, stdin={}",
        cfg.cpus, cfg.quanta, cfg.socket_path.display(), cfg.use_stdin
    );

    let mut stdout = std::io::stdout();

    let mut world = World::default();
    world.last_schedule = vec![None; cfg.cpus];

    let mut sched = Scheduler::new(cfg.cpus, cfg.quanta);

    if cfg.use_stdin {
        use std::io::{self, BufRead};
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let tf: TimeFrame = serde_json::from_str(&line)?;
            let tr = sched.on_tick(&mut world, tf.vtime, &tf.events)?;
            print_tick(&mut stdout, tr)?;
        }
        return Ok(());
    }

    let mut stream = UnixStream::connect(&cfg.socket_path)
        .with_context(|| format!("failed to connect to UDS {}", cfg.socket_path.display()))?;

    let mut buf: Vec<u8> = Vec::new();
    while let Some(tf) = read_one_json(&mut stream, &mut buf)? {
        let tr = sched.on_tick(&mut world, tf.vtime, &tf.events)?;
        print_tick(&mut stdout, tr)?;
    }

    Ok(())
}
