import asyncio
import datetime
import httpx
import gcsfs
from tqdm.asyncio import tqdm
from leap_data_management_utils.cmip_utils import IIDEntry, CMIPBQInterface

# there is an annoying bug that will make this fail when the number of tasks gathered exceeds `max_connections`
# https://github.com/encode/httpx/issues/1171

# Suggested workaround: https://github.com/encode/httpx/issues/1171#issuecomment-791614435

max_connections = 6
semaphore = asyncio.Semaphore(max_connections)
limits = httpx.Limits(max_connections=3, max_keepalive_connections=1)


async def fetch_batch_instance_ids(
    client: httpx.AsyncClient, url: str, params: dict, offset: int, batchsize: int
) -> list[str]:
    async with semaphore:
        response = await client.get(
            url, params=params | {"offset": offset, "limit": batchsize}
        )
        return [d["instance_id"] for d in response.json()["response"]["docs"]]


async def fetch_instance_ids(url, params):
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as client:
        # Initial response
        init_response = await client.get(url, params=params | {"offset": 0, "limit": 1})
        n_res = init_response.json()["response"]["numFound"]

        # fetch batches
        batchsize = 10000
        tasks = [
            asyncio.ensure_future(
                fetch_batch_instance_ids(client, url, params, i, batchsize)
            )
            for i in range(0, n_res, batchsize)
        ]

        retracted = await tqdm.gather(*tasks)

        retracted_flat = []
        for lst in retracted:
            retracted_flat.extend(lst)
        return retracted_flat


if __name__ == "__main__":
    table_id = "leap-pangeo.testcmip6.cmip6_consolidated_manual_testing"  # TODO: change to production table
    bq = CMIPBQInterface(table_id)
    params = {
        "type": "Dataset",
        "mip_era": "CMIP6",
        "replica": "none",  # this is horribly documented! But `none` should give both replicas and originals.
        "distrib": "true",
        "retracted": "true",
        "format": "application/solr+json",
        "fields": "instance_id",
    }
    url = "http://esgf-node.llnl.gov/esg-search/search"

    loop = asyncio.get_event_loop()
    retracted_iids = loop.run_until_complete(fetch_instance_ids(url, params))

    # Get all the latest entries
    df_all = bq.get_latest()

    # Find all entries that match ESGF retractions
    df_retracted = df_all[df_all["instance_id"].isin(retracted_iids)]

    # Find all the entries that are not marked as retracted yet
    to_retract = df_retracted[df_retracted["retracted"].isin([False])]

    # Print statistics and create report html
    to_retract.to_html("retraction_report.html")
    # upload the report to the bucket
    fs = gcsfs.GCSFileSystem()
    report_path = f"cmip6/cmip6-pgf-ingestion-test/reports/retraction_report_{datetime.datetime.utcnow().isoformat()}.html"
    fs.put("retraction_report.html", f"gs://{report_path}")

    ## Create IIDEntry objects for all the entries that need to be retracted
    iid_entries_to_retract = []
    for idx, row in to_retract.iterrows():
        iid_entry = IIDEntry(
            iid=row.instance_id,
            store=row.store,
            retracted=True,
            tests_passed=row.tests_passed,
        )
        iid_entries_to_retract.append(iid_entry)

    print(
        f"Got {len(retracted_iids)} retractions from ESGF.\n"
        f"{len(df_retracted)} of our stores are affected.\n"
        f"{len(to_retract)} stores will be newly marked as retracted.\n"
        f"See http://storage.googleapis.com/{report_path} for details\n"
    )

    # set values to retract in batches
    batchsize = (
        1000  # not sure what the max no of entries is that bq can handle at once
    )
    for batch_iid_entries in [
        iid_entries_to_retract[i : i + batchsize]
        for i in range(0, len(iid_entries_to_retract), batchsize)
    ]:
        bq.insert_multiple_iids(batch_iid_entries)
