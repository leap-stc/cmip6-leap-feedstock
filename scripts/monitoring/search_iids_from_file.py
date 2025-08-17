"""Search for CMIP6 Instance IDs (IIDs) in various catalogs."""

#!/usr/bin/env python3

import sys

import intake


def zstore_to_iid(zstore: str) -> str:
    """Convert a zstore path to an ESGF instance ID (IID)."""
    iid = ".".join(
        zstore.replace("gs://", "").replace(".zarr", "").replace(".", "/").split("/")[-11:-1]
    )
    if not iid.startswith("CMIP6"):
        iid = ".".join(
            zstore.replace("gs://", "").replace(".zarr", "").replace(".", "/").split("/")[-10:]
        )
    return iid


def search_iids(col_url: str, iids_requested: set) -> list:
    """
    Search a given CMIP6 catalog for requested IIDs.

    Args:
    ----
        col_url: The URL of the CMIP6 intake catalog.
        iids_requested: A set of IIDs to search for.

    Returns:
    -------
        A list of found IIDs.

    """
    col = intake.open_esm_datastore(col_url)
    iids_all = [zstore_to_iid(z) for z in col.df["zstore"].tolist()]
    return [iid for iid in iids_all if iid in iids_requested]


def load_iids_from_file(filepath: str) -> set:
    """
    Load IIDs from a text file, one IID per line.

    Args:
    ----
        filepath: The path to the file containing IIDs.

    Returns:
    -------
        A set of IIDs.

    """
    try:
        with open(filepath, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"Error reading IID file: {e}")
        sys.exit(1)


def main():
    """Search for requested IIDs in all CMIP6 catalogs."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Search for requested IIDs in all CMIP6 catalogs."
    )
    parser.add_argument("iid_file", help="Path to text file containing IIDs (one per line)")
    parser.add_argument("--count-only", action="store_true", help="Only print counts, not lists")
    parser.add_argument(
        "--search-string",
        type=str,
        default=None,
        help="Optional string to search for. Results matching are displayed in addition to general results.",
    )
    args = parser.parse_args()

    iids_requested = load_iids_from_file(args.iid_file)

    url_dict = {
        "qc": "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog.json",
        "non-qc": "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_noqc.json",
        "retracted": "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_retracted.json",
    }

    iids_found = []
    for catalog, url in url_dict.items():
        iids = search_iids(url, iids_requested)
        iids_found.extend(iids)
        if args.count_only:
            print(f"Found in {catalog=}: {len(iids)} matches")
        else:
            print(f"Found in {catalog=}: \n")
            for iid in sorted(iids):
                print(f"  - {iid}")

    missing_iids = list(iids_requested - set(iids_found))
    print(f"Total found: {len(iids_found)}")

    if args.count_only:
        print(f"\nStill missing {len(missing_iids)} of {len(iids_requested)} IIDs.")
    else:
        print(f"\nStill missing {len(missing_iids)} of {len(iids_requested)}:")
        for iid in sorted(missing_iids):
            print(f"  - {iid}")

    if args.search_string:
        searched_iids = [iid for iid in iids_requested if args.search_string in iid]
        if args.count_only:
            print(f"Total '{args.search_string}' IIDs found in request: {len(searched_iids)}")
        else:
            print(f"Total '{args.search_string}' IIDs found in request: {len(searched_iids)}")
            for iid in sorted(searched_iids):
                print(f"  - {iid}")


if __name__ == "__main__":
    main()
