import logging
from tqdm import tqdm
import gzip
import os
import shutil
import json
from typing import cast, NamedTuple, Iterator, Generator, List
from .UsageEventIterator import generate_instance_usage


SLOT_SIZE_IN_SECONDS = 300
USEC_IN_SEC = 10 ** 6


def timestamp_to_slot(timestamp: int) -> int:
    return timestamp // (SLOT_SIZE_IN_SECONDS * USEC_IN_SEC)


class TaskId(NamedTuple):
    collection_id: int
    instance_id: int


class MemInfo(NamedTuple):
    task: TaskId
    average_usage: float
    slot: int


def gen_meminfo(cluster_data_dir: str) -> Iterator[MemInfo]:
    for u in generate_instance_usage(cluster_data_dir):
        yield (
            MemInfo(
                task=TaskId(
                    collection_id=u.collection_id,
                    instance_id=u.instance_index,
                ),
                average_usage=u.average_usage.memory,
                slot=timestamp_to_slot(u.start_time),
            )
        )
    return


def cache_meminfo(cache_dir: str) -> Generator[None, MemInfo, None]:
    files = {}
    while True:
        m: MemInfo = yield
        if not m.slot in files:
            full_path = os.path.join(cache_dir, f"meminfo_cache_slot_{m.slot}.json.gz")
            files[m.slot] = gzip.open(full_path, 'xt')
        json.dump(m, files[m.slot])



def setup_cache(cluster_data_dir: str, cache_dir: str, refresh_cache: bool) -> None:
    if refresh_cache:
        logging.info(f"Deleting pre-existing cache in: {cache_dir}")
        shutil.rmtree(cache_dir, ignore_errors=True)
    elif os.path.isdir(cache_dir):
        logging.warning(
            f"Directory {cache_dir} already exists and no cache refresh asked - skipping cache creation"
        )
        return

    try:
        os.mkdir(cache_dir, 0o755)
    except FileExistsError as e:
        logging.error(f"File Exists {e}")
        raise
    except OSError as e:
        logging.error(f"Unexpected error: {e}")
        raise
    g = cache_meminfo(cache_dir)
    next(g)
    for m in tqdm(
        gen_meminfo(cluster_data_dir), desc="Generating meminfo cache", unit="items"
    ):
        g.send(m)


def start_simulation(results_dir: str, cache_dir: str):
    simulation = bootstrap_simulation()
def do_analyze(cluster_data_dir: str, cache_dir: str, result_path:str, refresh_cache: bool) -> None:
    cache_dir = os.path.abspath(cache_dir)
    cluster_data_dir = os.path.abspath(cluster_data_dir)
    setup_cache(cluster_data_dir, cache_dir, refresh_cache)
