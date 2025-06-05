use cap::Cap;
use std::{alloc, time::Duration};

use mountpoint_s3::{create_s3_client, parse_cli_args};

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

fn main() -> anyhow::Result<()> {
    let cli_args = parse_cli_args(true);

    // THIS WILL ONLY WORK WHEN USING `--foreground` DUE TO FORK WHEN DAEMONIZING
    std::thread::spawn(|| loop {
        std::thread::sleep(Duration::from_millis(1000));
        tracing::info!(target: mountpoint_s3_fs::metrics::TARGET_NAME, "rust_allocator.allocated_bytes: {}", &ALLOCATOR.allocated());
    });

    mountpoint_s3::run(create_s3_client, cli_args)
}
