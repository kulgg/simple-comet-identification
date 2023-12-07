#! /usr/bin/env python
import asyncio
import argparse
import logging

import pandas as pd
import requests
import concurrent.futures


async def process_file(input_file: str, api_url: str):
    comet_rev = ""
    with open(input_file, "r") as f:
        comet_rev = next(f)

    df = pd.read_csv(
        input_file,
        sep="\t",
        header=1,
    )

    domains_column_idx = len(df.columns)
    df.insert(domains_column_idx, column="domains", value="None", dtype=str)
    df.insert(domains_column_idx + 1, column="species", value="None", dtype=str)

    def _hit(i, row):
        req_url = "{}/peptides/{}".format(api_url, row["plain_peptide"])

        while True:
            try:
                r = requests.get(req_url)

                if r.status_code == 404:
                    break

                res = r.json()
                domain_names = set(map(lambda x: x["name"], res["domains"]))

                df.loc[i, "domains"] = ",".join(domain_names)
                df.loc[i, "species"] = ",".join(
                    set(map(lambda x: str(x), res["taxonomy_ids"]))
                )
                break
            except Exception as exception:
                logging.info("%s: exception %s", req_url, exception)

    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(executor, _hit, i, row) for i, row in df.iterrows()
        ]

        for response in await asyncio.gather(*tasks):
            pass

    with open(input_file, "w") as f:
        f.write(comet_rev)
        df.to_csv(f, sep="\t", index=False)

def get_cli() -> argparse.ArgumentParser:
    """Create the command line interface for the annotate script."""
    parser = argparse.ArgumentParser(
        description="Annotate a csv file"
    )
    parser.add_argument(
        "tsv_file",
        type=str,
        help="Path to tsv file",
    )
    parser.add_argument(
        "api_url",
        type=str,
        help="URL of MaCPepDB API",
    )
    return parser


def main():
    cli = get_cli()
    args = cli.parse_args()

    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(process_file(args.tsv_file, args.api_url)),
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == "__main__":
    main()