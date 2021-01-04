import hashlib
import logging
import binascii
import pathlib
import os
import json
import io
import enum
import gzip
from tqdm import tqdm
from typing import Iterator, List, TypeVar, Dict, Tuple

AppId = str


class LruAccessSimulator:
    # For each key - last_access_stamp
    app_id: AppId
    global_access_stamp: int
    keys: Dict[str, int]
    hist: Dict[int, int]

    def __init__(self, app_id: AppId, resolution: int):
        self.keys = {}
        self.hist = {}
        self.resolution = resolution
        self.global_access_stamp = 0
        self.app_id = app_id

    def access(self, key: str, value_size: int) -> None:
        self.global_access_stamp += value_size
        if not key in self.keys:
            self.keys[key] = self.global_access_stamp
        par = self.global_access_stamp - self.keys[key]
        hist_index = int(par / self.resolution)
        if not hist_index in self.hist:
            self.hist[hist_index] = 0
        self.hist[hist_index] += 1

    def delete(self, key: str, value_size: int) -> None:
        if not key in self.keys:
            return
        last_timestamp = self.keys[key]
        # We increase the timestamp of all keys that were last accessed *before* the item being deleted.
        # We can still be sure that the global_access_stamp >= the new timestamp for all those keys.
        for k in self.keys:
            if self.keys[k] < last_timestamp:
                self.keys[k] += value_size
        del self.keys[key]


class RequestType(enum.Enum):
    Get = 1
    Set = 2
    Delete = 3
    Add = 4
    Increment = 5
    Stats = 6
    Other = 7


class AppUsageInfo:
    app_id: AppId
    cur_size: int
    max_size: int
    count: int
    keys: Dict[str, int]

    def __init__(self, app_id: str):
        self.keys = {}
        self.cur_size = 0
        self.max_size = 0
        self.count = 0

    def access(self, key: str, value_size: int) -> None:
        if key in self.keys:
            return
        self.keys[key] = value_size
        self.cur_size += value_size
        self.max = max(self.max_size, self.cur_size)
        self.count += 1

    def delete(self, key: str) -> None:
        if not key in self.keys:
            return
        self.cur_size -= self.keys[key]
        self.count += 1
        del self.keys[key]


class Request:
    time_sec: float
    app_id: AppId
    req_type: RequestType
    key_size: int
    val_size: int
    key_id: str
    hit: bool

    def __init__(self, l: bytes):
        p = l.lstrip().split(b",")
        self.time_sec = float(p[0])
        self.app_id = p[1].decode("utf-8")
        self.req_type = RequestType(int(p[2]))
        self.key_size = int(p[3])
        self.val_size = int(p[4])
        self.key_id = p[5].decode("utf-8")
        self.hit = int(p[6]) == 1

    def __str__(self) -> str:
        s = "Time={}, AppId={}, ReqType={}, KeySize={}, ValSize={}, KeyId={}, Hit={}".format(
            self.time_sec,
            self.app_id,
            self.req_type,
            self.key_size,
            self.val_size,
            self.key_id,
            self.hit,
        )
        return s


def generate_requests(input_file: str) -> Iterator[Request]:
    with gzip.open(input_file, "r") as gz:
        with io.BufferedReader(gz) as f:  # type: ignore
            for l in f:
                r = Request(l)
                yield r
    return


def checksum_of_input(input_file: str) -> bool:
    expected_checksum = binascii.unhexlify(b"84a345a78397eb218ef14bf528193cee83263d95")
    hashing_obj = hashlib.new("sha1")
    block_size = 4096
    file_size = os.path.getsize(input_file)
    with open(input_file, "rb") as f:
        with tqdm(
            total=file_size, desc="Calculating file sha1", unit="bytes", unit_scale=True
        ) as progress_bar:
            block = f.read(block_size)
            while len(block) > 0:
                progress_bar.update(len(block))
                hashing_obj.update(block)
                block = f.read(block_size)
    file_digest = hashing_obj.digest()
    return expected_checksum == file_digest


def number_of_requests_in_trace(input_file: str) -> int:
    i = 0
    with gzip.open(
        input_file,
        "r",
    ) as gz:
        with io.BufferedReader(gz) as f:  # type: ignore
            for _ in tqdm(
                iterable=f,
                desc="Calculating number of entries from input",
                unit="Entries",
                unit_scale=True,
            ):
                i += 1
    return i


def get_top_apps(input_file: str, count: int) -> List[Tuple[AppId, int, int]]:
    apps: Dict[AppId, AppUsageInfo] = {}
    for r in tqdm(
        iterable=generate_requests(input_file),
        desc="Generating top apps",
        unit_scale=True,
        unit="Requests",
    ):

        if r.req_type in [RequestType.Get, RequestType.Set]:
            if not r.app_id in apps:
                apps[r.app_id] = AppUsageInfo(r.app_id)
            apps[r.app_id].access(r.key_id, r.val_size)
        elif r.req_type == RequestType.Delete:
            if not r.app_id in apps:
                continue
            apps[r.app_id].delete(r.key_id)
    logging.info("Sorting apps by required memory size...")
    top_apps = sorted(list(apps.values()), key=lambda x: x.app_id)
    return [(app.app_id, app.count, app.max_size) for app in top_apps]


def get_top_apps_requests(top_apps: List[AppId], input_file: str) -> Iterator[Request]:
    return filter(
        lambda x: x.app_id in top_apps
        and x.req_type in [RequestType.Get, RequestType.Set, RequestType.Delete],
        generate_requests(input_file),
    )


def build_mrcs(
    top_apps: List[AppId], total_entries: int, input_file: str
) -> Dict[AppId, LruAccessSimulator]:
    mrc_resolution = 2 ** 10
    lrus: Dict[AppId, LruAccessSimulator] = {}
    for t in top_apps:
        lrus[t] = LruAccessSimulator(t, mrc_resolution)
    for req in tqdm(
        iterable=get_top_apps_requests(top_apps, input_file),
        total=total_entries,
        desc="Building MRCs",
        unit="requests",
        unit_scale=True,
    ):
        if req.req_type == RequestType.Delete:
            lrus[req.app_id].delete(req.key_id, req.value_size)
            continue
        lrus[req.app_id].access(req.key_id, req.value_size)
    return lrus


def load_top_apps_cache_file(file_path: str) -> List[Tuple[AppId, int, int]]:
    with gzip.open(file_path, "r") as f:
        return json.load(f)  # type: ignore


def store_top_apps_to_cache_file(
    file_path: str, top_apps: List[Tuple[AppId, int, int]]
) -> None:
    with gzip.open(file_path, "w") as f:
        json.dump(f, top_apps)  # type: ignore


def gen_mrc(
    output_file: str,
    number_of_mrcs: int,
    input_file: str,
    skip_checksum: bool,
    overwrite_existing: bool,
    top_apps_file: str,
) -> None:
    if not os.path.isfile(input_file):
        logging.critical(
            f"Input file ({input_file}) does not exist or does not appear to be a regular file"
        )
        raise ValueError(
            f"Input file ({input_file}) does not exist or does not appear to be a regular file"
        )

    if not overwrite_existing and os.path.exists(output_file):
        logging.critical(
            f"Output file ({output_file}) already exists, either remove it or use --overwrite-output flag"
        )
        raise ValueError(f"Output file ({output_file}) already exists")

    top_apps = None
    if os.path.isfile(top_apps_file):
        logging.info(f"Found top apps cache file {top_apps_file} -- loading!")
        top_apps = load_top_apps_cache_file(top_apps_file)
    elif os.path.exists(top_apps_file):
        logging.error(
            f"given top apps file path ({top_apps_file}) isn't a regular file"
        )
        raise ValueError(
            f"given top apps file path ({top_apps_file}) isn't a regular file"
        )
    if not skip_checksum:
        if not checksum_of_input(input_file):
            logging.critical(
                f"Input file ({input_file}) checksum is wrong, make sure you have downloaded the correct file, or use --skip-checksum"
            )
            raise ValueError("Input file ({input_file}) has incorrect checksum")
        logging.info("Checksum - OK!")
    #  logging.info("Trying to estimate trace size...")
    #  number_of_lines = number_of_requests_in_trace(input_file)
    logging.info(f"Getting top {number_of_mrcs} apps")
    #  get_top_apps(input_file, number_of_mrcs, number_of_lines)
    if top_apps == None:
        logging.info("Generating top apps now")
        top_apps = get_top_apps(input_file, number_of_mrcs)
        logging.info("Finished generating top apps.")
        logging.info(f"Writing top apps to file: {top_apps_file}")
        store_top_apps_to_cache_file(top_apps_file, top_apps)
    return
