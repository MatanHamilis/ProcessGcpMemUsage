import os
import gzip
from typing import NamedTuple, Iterator
import typing
import logging
from pricing_simulator.proto import clusterdata_trace_format_v3_pb2
from google.protobuf.json_format import Parse as ProtobufParse


def generate_instance_usage(
    directory: str,
) -> Iterator[clusterdata_trace_format_v3_pb2.InstanceUsage]:
    files = filter(lambda x: x.startswith("instance_usage"), os.listdir(directory))
    for path in files:
        full_path = os.path.join(directory, path)
        with gzip.open(full_path, "rb") as f:
            for line in f.readlines():
                u = clusterdata_trace_format_v3_pb2.InstanceUsage()
                ProtobufParse(line, u)
                yield u
    return
