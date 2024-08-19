from datetime import datetime, timezone
import dataclasses
import json
import logging
import os
import subprocess
import tempfile

import hydra
from omegaconf import DictConfig

logging.basicConfig(
    level=os.environ.get('LOGLEVEL', 'INFO').upper()
)

log = logging.getLogger(__name__)

MOUNT_DIRECTORY = "s3"
MP_LOGS_DIRECTORY = "mp_logs/"

@dataclasses.dataclass
class Metadata(object):
    """
    Metadata for the benchmark run.
    """
    start_time: str
    end_time: str
    elapsed: str
    mp_version: str

def _mount_mp(cfg: DictConfig, mount_dir :str) -> str:
    """
    Mount an S3 bucket using Mountpoint, using the configuration to apply Mountpoint arguments.

    Returns Mountpoint version string.
    """
    mountpoint_binary = os.path.join(
        hydra.utils.get_original_cwd(),
        cfg['mountpoint_binary'],
    )

    os.makedirs(MP_LOGS_DIRECTORY, exist_ok=True)

    bucket = cfg['s3_bucket']

    mountpoint_version_output = subprocess.check_output([mountpoint_binary, "--version"]).decode("utf-8")
    log.info("Mountpoint version: %s", mountpoint_version_output.strip())

    subprocess_args = [
        mountpoint_binary,
        bucket,
        mount_dir,
        f"--metadata-ttl={cfg['metadata_ttl']}",
        "--log-metrics",
        f"--log-directory={MP_LOGS_DIRECTORY}",
        "--write-part-size=16777216", # 16MiB
        "--upload-checksums=off", # testing for S3 on edge
    ]
    if cfg['s3_prefix'] is not None:
        subprocess_args.append(f"--prefix={cfg['s3_prefix']}")
    if cfg['mountpoint_debug']:
        subprocess_args.append("--debug")
    if cfg['mountpoint_debug_crt']:
        subprocess_args.append("--debug-crt")
    if cfg['fuse_threads'] is not None:
        subprocess_args.append(f"--max-threads={cfg['fuse_threads']}")
    if cfg['network'] is not None:
        network = cfg['network']
        for network_interface in network['interface_names']:
            subprocess_args.append(f"--bind={network_interface}")
        if network['maximum_throughput_gbps'] is not None:
            subprocess_args.append(f"--maximum-throughput-gbps={network['maximum_throughput_gbps']}")

    log.info(f"Mounting S3 bucket {bucket} using the following command: %s", " ".join(subprocess_args))
    output = subprocess.check_output(subprocess_args, env={"PID_FILE": "mount-s3.pid"})
    log.info("From Mountpoint: %s", output.decode("utf-8").strip())

    with open("mount-s3.pid") as pid_file:
        pid = pid_file.read().rstrip()
    log.debug("Mountpoint PID: %s", pid)

    if cfg['wait_for_perf_attach']:
        input("Press Enter to continue...") # so that I can attach perf
        # subprocess_args = [
        #     "/usr/bin/sudo",
        #     "/usr/bin/perf",
        #     "record",
        #     "-F",
        #     "99", # Hz
        #     "-p",
        #     pid,
        # ]
        # log.debug(f"Starting perf profile recording using the following command: %s", " ".join(subprocess_args))
        # try:
        #     _ = subprocess.check_output(subprocess_args)
        # except subprocess.CalledProcessError as e:
        #     log.error("Failed to start perf profile recording: %s", e)
        #     raise e

    return mountpoint_version_output

def _run_fio(cfg: DictConfig, mount_dir: str) -> tuple[datetime, datetime]:
    """
    Run the FIO workload against the file system.

    Returns the start and end times of the workload.
    """
    FIO_BINARY = "/usr/bin/fio"
    subprocess_args = [
        FIO_BINARY,
        "--output=fio-output.json",
        "--output-format=json",
        "--eta=never",
        f"--directory={mount_dir}",
        hydra.utils.to_absolute_path("sequential_read.fio"),
    ]
    subprocess_env = {
        "NUMJOBS": str(cfg['application_workers']),
        "SIZE_GIB": str(100),
        "DIRECT": str(1 if cfg['direct_io'] else 0),
    }
    start_time = datetime.now(tz=timezone.utc)
    log.debug(f"Running FIO with args: %s; env: %s", subprocess_args, subprocess_env)
    log.info(f"FIO job starting now at %s", start_time)
    subprocess.check_output(subprocess_args, env=subprocess_env)
    end_time = datetime.now(tz=timezone.utc)
    log.info(f"FIO job complete now at %s", end_time)
    return start_time, end_time


def _run_dd(cfg: DictConfig, mount_dir: str) -> tuple[datetime, datetime]:
    """
    Run the DD workload against the file system.

    Returns the start and end times of the workload.
    """

    BASH_BINARY="/usr/bin/bash"

    subprocess_args = [
        BASH_BINARY,
        hydra.utils.to_absolute_path("dd_bench.bash"),
        mount_dir,
        str(cfg['application_workers']),
        str(cfg['direct_io']),
    ]
    subprocess_env = {}
    start_time = datetime.now(tz=timezone.utc)
    log.debug(f"Running DD workload script with args: %s; env: %s", subprocess_args, subprocess_env)
    log.info(f"DD workload starting now at %s", start_time)
    subprocess.check_output(subprocess_args, env=subprocess_env)
    end_time = datetime.now(tz=timezone.utc)
    log.info(f"DD workload complete now at %s", end_time)
    return start_time, end_time

def _run_workload(cfg: DictConfig, mount_dir: str) -> tuple[datetime, datetime]:
    if cfg['workload'] == "fio":
        return _run_fio(cfg, mount_dir)
    elif cfg['workload'] == "dd":
        return _run_dd(cfg, mount_dir)
    else:
        raise ValueError(f"Unknown workload: {cfg['workload']}")

def _unmount_mp(mount_dir: str) -> None:
    subprocess.check_output(["/usr/bin/umount", mount_dir])
    log.info(f"{mount_dir} unmounted")

def _collect_logs() -> None:
    """
    Collect all logs and move them to the output directory. Drop the old directory.
    """
    dir_entries = os.listdir(MP_LOGS_DIRECTORY)
    assert len(dir_entries) == 1, f"Expected exactly one log file in {MP_LOGS_DIRECTORY}"
    old_log_dir = os.path.join(MP_LOGS_DIRECTORY, dir_entries[0])
    new_log_path = "mountpoint-s3.log"
    log.debug(f"Renaming {old_log_dir} to {new_log_path}")
    os.rename(old_log_dir, new_log_path)
    os.rmdir(MP_LOGS_DIRECTORY)

def _write_metadata(metadata: Metadata) -> None:
    with open("metadata.json", "w") as f:
        json_str = json.dumps(dataclasses.asdict(metadata), default=str)
        f.write(json_str)

def _postprocessing(metadata: Metadata) -> None:
    _collect_logs()
    _write_metadata(metadata)

@hydra.main(version_base=None, config_path="conf", config_name="config")
def run_experiment(cfg: DictConfig) -> None:
    """
    At a high level, we want to mount the S3 bucket using Mountpoint,
    run a synthetic workload against Mountpoint while capturing metrics, end the load and unmount the bucket.

    We should collect all of the logs and metric and dump them in the output directory.
    """
    log.info("Experiment starting")
    mount_dir = tempfile.mkdtemp(suffix=".mountpoint-s3")
    try:
        mp_version = _mount_mp(cfg, mount_dir)
        start_time, end_time = _run_workload(cfg, mount_dir)
        metadata = Metadata(start_time=start_time, end_time=end_time, elapsed=end_time-start_time, mp_version=mp_version)
    finally:
        _unmount_mp(mount_dir)
        os.rmdir(mount_dir)
    _postprocessing(metadata)
    log.info("Experiment complete")

if __name__ == "__main__":
    run_experiment()
