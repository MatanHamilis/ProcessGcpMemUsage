import argparse
import logging
from pricing_simulator.download.download import do_download as _do_download
from pricing_simulator.analyze.analyze import do_analyze as _do_analyze
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
    _do_analyze(args.cluster_data_dir, args.cache_dir)


if __name__ == "__main__":
    main()
