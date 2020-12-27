import logging
import os
import urllib.request
import requests
from tqdm import tqdm
import xml.etree.ElementTree
from defusedxml.ElementTree import fromstring
from typing import List

bucket_name: str = "clusterdata_2019_a"
gcp_public_storage_base_url: str = "https://storage.googleapis.com"
xmlns = "http://doc.s3.amazonaws.com/2006-03-01"


def gen_bucket_url(bucket_name: str) -> str:
    return "/".join([gcp_public_storage_base_url, bucket_name])


def gen_file_url(bucket_name, file_name: str) -> str:
    return "/".join([gcp_public_storage_base_url, bucket_name, file_name])


def fetch_xml(url: str) -> xml.etree.ElementTree.Element:
    logging.debug(f"Fetching xml from {url}")
    page = urllib.request.urlopen(url)
    contents = page.read()
    logging.debug(f"Fetched XML of length: {len(contents)}")
    return fromstring(contents)


def get_all_available_files() -> List[str]:
    bucket_xml = fetch_xml(gen_bucket_url(bucket_name))
    namespace_dict = {"": xmlns}
    return [i.text for i in bucket_xml.findall("./Contents/Key", namespace_dict)]


def get_list_of_files_to_download(count: int) -> List[str]:
    return list(
        filter(lambda x: x.startswith("instance_usage"), get_all_available_files())
    )[:count]


def download_file(url: str, dest_file: str) -> None:
    request = requests.get(url, stream=True)
    block_size = 2 ** 18
    with open(dest_file, "wb") as f:
        total_length = int(request.headers.get("Content-Length"))
        for chunk in tqdm(
            request.iter_content(chunk_size=block_size),
            total=int((total_length / block_size) + 1),
            desc=f"Downloading {os.path.basename(dest_file)}",
            unit_scale=float(block_size) / (2 ** 20),
            mininterval=0.5,
            unit="MB",
        ):
            if chunk:
                f.write(chunk)
        f.flush()


def do_download(dest_dir: str, count: int, force: bool) -> None:
    dest_dir = os.path.abspath(dest_dir)
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)
    elif not os.path.isdir(dest_dir):
        logging.critical(f"Path {dest_dir} exists and it's not a directory.")
        exit(-1)
    files_to_download = get_list_of_files_to_download(count)
    for f in files_to_download:
        full_local_path = os.path.join(dest_dir, f)
        if not force and os.path.exists(full_local_path):
            logging.info(f"File {full_local_path} exists - skipping download")
            continue
        remote_path = gen_file_url(bucket_name, f)
        download_file(remote_path, full_local_path)
