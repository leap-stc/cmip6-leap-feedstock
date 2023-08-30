from bigquery_interface import BQInterface, IIDEntry
import pandas as pd
from pangeo_forge_esgf.utils import CMIP6_naming_schema


def _maybe_prepend_dash(s: str):
    if not "-" in s:
        return "-" + s
    else:
        return s


def convert_bq_to_cmip6_df(table_id: str) -> pd.DataFrame:
    """Convert a bigquery table to a pandas dataframe
    conforming to the old pangeo cmip6 catalog format
    :param table_id: BigQuery table ID
    :returns: pandas dataframe
    """
    bq = BQInterface(table_id)
    latest_rows = bq.get_latest()

    iid_facets = CMIP6_naming_schema.split(".")
    df = pd.DataFrame()
    # some legit pandas magic here: https://stackoverflow.com/a/39358924
    df[iid_facets] = latest_rows["instance_id"].str.split(".", expand=True)
    df["zstore"] = "gs://" + latest_rows["store"]

    # add a dummy row to enable splitting of the variant_label
    modified = df.iloc[[-1]]
    modified["variant_label"] = "test-stuff"
    df = pd.concat([df, pd.DataFrame(modified)])

    # expand member_id into dcpp_init_year and variant_label
    # (relabelling variant_label to member_id. I think this is wrong,
    # but keeping it for backwards compatibility)
    df[["dcpp_init_year", "member_id"]] = (
        df["variant_label"]
        .apply(lambda s: _maybe_prepend_dash(s))
        .str.split("-", expand=True)
    )

    # remove the dummy row again
    df = df.iloc[:-1]

    # order the columns in the order of the old csv (not sure if this is necessary)
    df = df[
        [
            "activity_id",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "zstore",
            "dcpp_init_year",
            "version",
        ]
    ]
    return df


# I need a query to get the latest row for each instance_id
table_id = "leap-pangeo.testcmip6.cmip6_feedstock_test2"
table_id_nonqc = "leap-pangeo.testcmip6.cmip6_feedstock_test2_nonqc"

df_qc = convert_bq_to_cmip6_df(table_id)
df_nonqc = convert_bq_to_cmip6_df(table_id_nonqc)

# write to csv
df_qc.to_csv("leap-pangeo-cmip6-test.csv", index=False)
df_nonqc.to_csv("leap-pangeo-cmip6-noqc-test.csv", index=False)
