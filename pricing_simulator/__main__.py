import argparse
import logging
from pricing_simulator.download.download import do_download as _do_download
from pricing_simulator.analyze.analyze import do_analyze as _do_analyze
from pricing_simulator.memcachier_mrc import gen_mrc
import sys


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s ---- %(levelname)s: %(message)s",
    datefmt="%d/%m/%y %H:%M:%S %z",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="count", default=0)
    subparsers = parser.add_subparsers(help="sub-command help", dest="cmd")
    subparsers.required = True

    gen_mrc_parser = subparsers.add_parser("gen-mrc")
    gen_mrc_parser.add_argument(
        "--input-file",
        required=True,
        help="Path to a file downloaded from https://drive.google.com/uc?id=10zc2KjzJxtCcv8ExidRYfo55cjesEiFq",
    )
    gen_mrc_parser.add_argument(
        "--out-path", required=True, help="Output file for generated MRC information"
    )
    gen_mrc_parser.add_argument(
        "--count", type=int, default=20, help="Number of MRCs to generate [default: 20]"
    )
    gen_mrc_parser.add_argument(
        "--skip-checksum",
        default=False,
        action="store_true",
        help="Skip checksum of input file",
    )
    gen_mrc_parser.add_argument(
        "--overwrite-output",
        action="store_true",
        default=False,
        help="Overwrite existing output file if exists",
    )
    gen_mrc_parser.add_argument(
        "--top-apps-file",
        type=str,
        default=".topapps.json.gz",
        help="This file will be used to load/store a per-app cache info to save future computation. If the file is found, its contents will be used, otherwise the full per-app information generation process will takes place and results will be written to this file [default: .topapps.cache]",
    )
    gen_mrc_parser.set_defaults(func=do_gen_mrc)

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("--dest-dir", required=False, default=".")
    download_parser.add_argument("--count", required=True, type=int, default=".")
    download_parser.add_argument(
        "--force", action="store_true", help="If set, overwrites existing files"
    )
    download_parser.set_defaults(func=do_download)

    analyze_parser = subparsers.add_parser("analyze")
    analyze_parser.add_argument("--cluster-data-dir", required=True, metavar="d")
    analyze_parser.add_argument("--cache-dir", required=False, default="cache")
    analyze_parser.add_argument("--results-dir", required=False, default="results")
    analyze_parser.add_argument(
        "--refresh-cache",
        required=False,
        default=False,
        action="store_true",
        help="Rebuild cache if already exists",
    )
    analyze_parser.set_defaults(func=do_analyze)
    args = parser.parse_args()
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)
    args.func(args)


def do_download(args: argparse.Namespace) -> None:
    logging.info("COMMAND: download ")
    logging.info(f"COUNT:\t{args.count} ")
    logging.info(f"DEST DIR:\t{args.dest_dir} ")
    logging.info(f"FORCE:\t{args.force}")
    _do_download(args.dest_dir, args.count, args.force)


def do_analyze(args: argparse.Namespace) -> None:
    logging.info("COMMAND: analyze")
    logging.info(f"CLUSTER DATA DIR:\t{args.cluster_data_dir}")
    logging.info(f"CACHE DIR:\t{args.cache_dir}")
    logging.info(f"RESULTS DIR:\t{args.results_dir}")
    logging.info(f"REFRESH CACHE:\t{args.refresh_cache}")
    _do_analyze(
        args.cluster_data_dir, args.cache_dir, args.results_dir, args.refresh_cache
    )


def do_gen_mrc(args: argparse.Namespace) -> None:
    logging.info("COMMAND: gen_mrc")
    logging.info(f"INPUT FILE:\t{args.input_file}")
    logging.info(f"OUT PATH:\t{args.out_path}")
    logging.info(f"COUNT:\t{args.count}")
    logging.info(f"SKIP CHECKSUM:\t{args.skip_checksum}")
    logging.info(f"OVERWRITE OUTPUT:\t{args.overwrite_output}")
    logging.info(f"TOP APPS FILE:\t{args.top_apps_file}")
    gen_mrc(
        args.out_path,
        args.count,
        args.input_file,
        args.skip_checksum,
        args.overwrite_output,
        args.top_apps_file,
    )


if __name__ == "__main__":
    main()
