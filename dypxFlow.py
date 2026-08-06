#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from loguru import logger
from chris_plugin import chris_plugin, PathMapper
import pandas as pd
from typing import List, Dict
from chrisClient import ChrisClient
from notifications import ConsoleChannel, NotificationEvent, NotificationManager, NotificationContext
from chris_notification import Notification
from chris_notification_channel import ChRISNotificationChannel

import pfdcm
import sys
import os
import concurrent.futures
import asyncio

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

__version__ = '1.1.7'

DISPLAY_TITLE = r"""
       _           _                ______ _               
      | |         | |               |  ___| |              
 _ __ | |______ __| |_   _ _ ____  _| |_  | | _____      __
| '_ \| |______/ _` | | | | '_ \ \/ /  _| | |/ _ \ \ /\ / /
| |_) | |     | (_| | |_| | |_) >  <| |   | | (_) \ V  V / 
| .__/|_|      \__,_|\__, | .__/_/\_\_|   |_|\___/ \_/\_/  
| |                   __/ | |                              
|_|                  |___/|_|                              
"""


parser = ArgumentParser(description='A dynamic ChRIS plugin to run anonymization pipeline',
                        formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument(
    '-V', '--version',
    action='version',
    version=f'%(prog)s {__version__}'
)
parser.add_argument(
    "--pattern",
    default="**/*csv",
    help="""
            pattern for file names to include.
            Default is **/*csv.""",
)
parser.add_argument(
    "--pluginInstanceID",
    default="",
    help="plugin instance ID from which to start analysis",
)
parser.add_argument(
    "--CUBEurl",
    default="http://localhost:8000/api/v1/",
    help="CUBE URL. Please include api version in the url endpoint."
)
parser.add_argument(
    "--CUBEtoken",
    default="",
    help="CUBE/ChRIS user token"
)
parser.add_argument(
    "--maxThreads",
    default=4,
    help="max number of parallel threads"
)
parser.add_argument(
    "--thread",
    help="use threading to branch in parallel",
    dest="thread",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--wait",
    help="wait for nodes to reach finished state",
    dest="wait",
    action="store_true",
    default=False,
)
parser.add_argument(
    '--PFDCMurl',
    default='',
    type=str,
    help='endpoint URL of pfdcm. Please include api version in the url endpoint.'
)
parser.add_argument(
    '--PACSname',
    default='MINICHRISORTHANC',
    type=str,
    help='name of the PACS'
)
parser.add_argument(
    '--recipients',
    default='',
    type=str,
    help='comma separated valid email recipient addresses'
)
parser.add_argument(
    '--SMTPServer',
    default='mailsmtp4.childrenshospital.org',
    type=str,
    help='valid email server'
)
parser.add_argument(
    '--largeSequenceSize',
    default=10000,
    type=int,
    help='sequence size that will be considered as large sequence for delayed polling'
)
parser.add_argument(
    '--largeSequencePollInterval',
    default=10,
    type=int,
    help='poll interval time for large sequences (in minutes)'
)


def configure_notifications(options):
    mgr = NotificationManager.instance()

    mgr.register_channel(ConsoleChannel())
    mgr.register_channel(
        ChRISNotificationChannel(
            cube_url=options.CUBEurl,
            cube_token=options.CUBEtoken,
            smtp_server=options.SMTPServer,
        )
    )

    # Edit this (or load_routing_from_file(...)) to change who gets notified
    # for which event - no code changes needed elsewhere.
    mgr.configure_routing(NotificationEvent.START, ["console"])
    mgr.configure_routing(NotificationEvent.SUCCESS, ["console", "chris"])
    mgr.configure_routing(NotificationEvent.ERROR, ["console", "chris"])
    mgr.configure_routing(NotificationEvent.END, ["console"])

    return mgr


def skip_condition(row):
    # Skip rows where starting column says 'no'
    if row[0].lower() == 'yes':
        return False
    return True
# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser=parser,
    title='A dynamic ChRIS plugin for PACS',
    category='',                 # ref. https://chrisstore.co/plugins
    min_memory_limit='100Mi',    # supported units: Mi, Gi
    min_cpu_limit='1000m',       # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit=0              # set min_gpu_limit=1 to enable GPU
)
def main(options: Namespace, inputdir: Path, outputdir: Path):
    """
    *ChRIS* plugins usually have two positional arguments: an **input directory** containing
    input files and an **output directory** where to write output files. Command-line arguments
    are passed to this main method implicitly when ``main()`` is called below without parameters.

    :param options: non-positional arguments parsed by the parser given to @chris_plugin
    :param inputdir: directory containing (read-only) input files
    :param outputdir: directory where to write output files
    """

    print(DISPLAY_TITLE)

    log_file = outputdir / "terminal.log"
    logger.add(str(log_file))

    if not health_check(options): sys.exit("An error occurred during health check!")

    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.pattern)
    for input_file, output_file in mapper:

        df = pd.read_csv(input_file, dtype=str)
        # A custom row skipping condition can be added here to skip rows from the csv file
        #,skiprows=lambda x: 0 if x == 0 else skip_condition(pd.read_csv(input_file, nrows=x).iloc[-1].tolist()) )
        # 1 Remove rows with all NaN values
        df.dropna(how='all', inplace=True)

        # 2 Replace NaN values with empty strings
        df_clean = df.fillna('')
        l_job = create_query(df_clean)
        d_df = []
        pipeline_errors = False
        if int(options.thread):
            with concurrent.futures.ThreadPoolExecutor(max_workers=int(options.maxThreads)) as executor:
                results: Iterator = executor.map(lambda t: register_and_anonymize(options, t, options.wait), l_job)

            # Wait for all tasks to complete
            executor.shutdown(wait=True)
        else:
            for d_job in l_job:
                response = asyncio.run(register_and_anonymize(options, d_job))
                row = d_job["raw"]
                row.update(d_job["push"])
                row["status"] = response['status']
                d_df.append(row)
                if response.get('error'):
                    pipeline_errors = True

        # Write output CSV
        out_csv = outputdir / input_file.name
        pd.DataFrame(d_df).to_csv(out_csv, index=False)

        LOG(f"Sending notification to user(s)")
        try:
            notification = Notification(options.CUBEurl, options.CUBEtoken)
            notification.run_notification_plugin(pv_id=options.pluginInstanceID,
                                                 msg="Pipeline finished running",
                                                 rcpts=options.recipients,
                                                 smtp=options.SMTPServer,
                                                 search_data="")
        except Exception as ex:
            LOG(f"Error occurred: {ex}")
        if pipeline_errors:
            LOG(f"ERROR while running pipelines.")
            sys.exit(1)


if __name__ == '__main__':
    main()

async def register_and_anonymize(
    options: Namespace,
    d_job: dict,
    wait: bool = False
):
    """
    Run PACS query pipeline using the job dictionary
    """

    # Enrich job (non-destructive if already present)
    d_job.setdefault("pull", {
        "url": options.PFDCMurl,
        "pacs": options.PACSname
    })
    d_job.setdefault("notify", {
        "recipients": options.recipients,
        "smtp_server": options.SMTPServer
    })
    d_job.setdefault("relay", {
        "largeSequenceSize": options.largeSequenceSize,
        "largeSequencePollInterval": options.largeSequencePollInterval
    })

    LOG(d_job)

    # If already pushed, nothing to do
    if d_job["push"].get("status"):
        return d_job["push"]

    cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)

    # Run pipeline
    d_ret = await cube_con.anonymize(d_job, options.pluginInstanceID)

    # Optional neuro pull
    search = d_job.get("search", {})
    neuro = search.get("neuro")

    if neuro:
        filter_str: str = f"*{search['PatientID']}*/*{search['StudyDate']}*/*{search['sequence']}*/**"
        feed_name: str = f"{search['PatientID']}_{search['StudyDate']}_{search['sequence']}"
        d_ret = await cube_con.neuro_pull(
            neuro,
            feed_name,
            filter_str,
            d_job
        )

    return d_ret



def _get_or_env(value, env_key):
    return value or os.environ[env_key]


def health_check(options) -> bool:
    """
    Check if connections to PFDCM and CUBE are valid.
    """

    LOG("health_check starting")

    try:
        mgr = configure_notifications(options)
        LOG(f"Manager configured. Channels: {list(mgr.channels.keys())}")
        LOG(f"ERROR routes to: {mgr.routing.get(NotificationEvent.ERROR, [])}")
    except Exception as e:
        LOG(f"ERROR configuring notifications: {e}")
        raise

    try:
        # Resolve required options from env if missing
        options.pluginInstanceID = _get_or_env(
            options.pluginInstanceID, 'CHRIS_PREV_PLG_INST_ID'
        )
        options.CUBEtoken = _get_or_env(
            options.CUBEtoken, 'CHRIS_USER_TOKEN'
        )
    except Exception as ex:
        LOG(ex)
        # run_extra can't be built yet because options weren't resolved
        # So either skip ChRIS for this error, or build it without plugin_instance_id
        mgr.notify(NotificationContext(
            event=NotificationEvent.ERROR,
            pipeline_name="health_check",
            step_name="resolve_options",
            message=f"Failed to resolve required options: {ex}",
            error=ex,
            extra={},  # Can't include plugin_instance_id yet
        ))
        return False

    # NOW build run_extra with resolved options
    run_extra = {
        "plugin_instance_id": options.pluginInstanceID,
        "recipients": options.recipients,
    }

    # CUBE health check
    try:
        cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
        cube_con.health_check()
    except Exception as ex:
        LOG(ex)
        mgr.notify(NotificationContext(
            event=NotificationEvent.ERROR,
            pipeline_name="health_check",
            step_name="cube_connection",
            message=f"CUBE health check failed ({options.CUBEurl}): {ex}",
            error=ex,
            extra=run_extra,  # ← now has real values
        ))
        return False

    # PFDCM health check
    try:
        pfdcm.health_check(options.PFDCMurl)
    except Exception as ex:
        LOG(ex)
        mgr.notify(NotificationContext(
            event=NotificationEvent.ERROR,
            pipeline_name="health_check",
            step_name="pfdcm_connection",
            message=f"PFDCM health check failed ({options.PFDCMurl}): {ex}",
            error=ex,
            extra=run_extra,  # ← now has real values
        ))
        return False

    return True


def create_query(df: pd.DataFrame) -> List[Dict]:
    """
    Efficiently serializes the data table to create a job dictionary
    """

    columns = list(df.columns)

    # Precompute column classifications
    search_cols = []
    anon_cols = []

    for col in columns:
        col_lower = str(col).lower()
        if any(x in col_lower for x in ("search", "neuro", "sequence")):
            # Precompute the transformed key once
            key = col.split('.')[0].split('_')[1]
            search_cols.append((col, key))

        if any(x in col_lower for x in ("status", "folder", "path")):
            anon_cols.append(col)

    jobs = []

    for row in df.itertuples(index=False, name=None):
        row_dict = dict(zip(columns, row))

        # Search
        search = {
            key: row_dict[col]
            for col, key in search_cols
        }

        # Push / Anonymization
        push = {
            col: row_dict[col]
            for col in anon_cols
        }

        # Raw
        raw = dict(row_dict.items())

        jobs.append({
            "search": search,
            "push": push,
            "raw": raw
        })

    return jobs



