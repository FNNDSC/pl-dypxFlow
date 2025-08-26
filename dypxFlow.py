#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from pflog import pflog
from loguru import logger
from chris_plugin import chris_plugin, PathMapper
import pandas as pd
from collections import ChainMap
from chrisClient import ChrisClient
import pfdcm
import sys
import os
import concurrent.futures

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

__version__ = '1.0.8'

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

    # Typically it's easier to think of programs as operating on individual files
    # rather than directories. The helper functions provided by a ``PathMapper``
    # object make it easy to discover input files and write to output files inside
    # the given paths.
    #
    # Refer to the documentation for more options, examples, and advanced uses e.g.
    # adding a progress bar and parallelism.
    log_file = os.path.join(options.outputdir, 'terminal.log')
    logger.add(log_file)
    if not health_check(options): sys.exit("An error occurred!")

    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.pattern)
    for input_file, output_file in mapper:

        df = pd.read_csv(input_file, dtype=str) #,skiprows=lambda x: 0 if x == 0 else skip_condition(pd.read_csv(input_file, nrows=x).iloc[-1].tolist()) )
        #1 Remove rows with all NaN values
        df.dropna(how='all', inplace=True)

        #2 Replace NaN values with empty strings
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
                response = register_and_anonymize(options, d_job)
                row = d_job["raw"]
                row.update(d_job["push"])
                row["status"] = response['status']
                d_df.append(row)
                if response.get('error'):
                    pipeline_errors = True

        csv_file = os.path.join(options.outputdir, input_file.name)
        df = pd.DataFrame(d_df)
        df.to_csv(csv_file, index=False)
        if pipeline_errors:
            LOG(f"ERROR while running pipelines.")
            sys.exit(1)


if __name__ == '__main__':
    main()
def register_and_anonymize(options: Namespace, d_job: dict, wait: bool = False):
    """
    1) Search through PACS for series and register in CUBE
    2) Run anonymize and push workflow on the registered series
    """
    resp = {}
    d_job["pull"] = {
        "url": options.PFDCMurl,
        "pacs": options.PACSname
    }
    LOG(d_job)
    if not d_job["push"]["status"]:
        cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
        d_ret = cube_con.anonymize(d_job, options.pluginInstanceID)
    else:
        d_ret = d_job["push"]


    return d_ret


def health_check(options) -> bool:
    """
    check if connections to pfdcm and CUBE is valid
    """
    try:
        if not options.pluginInstanceID:
            options.pluginInstanceID = os.environ['CHRIS_PREV_PLG_INST_ID']
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # create connection object
        if not options.CUBEtoken:
            options.CUBEtoken = os.environ['CHRIS_USER_TOKEN']
        cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
        cube_con.health_check()
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # pfdcm health check
        pfdcm.health_check(options.PFDCMurl)
    except Exception as ex:
        LOG(ex)
        return False
    return True


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
def create_query(df: pd.DataFrame):
    columns = df.columns
    col_locs = {col: i for i, col in enumerate(columns)}

    # Classify columns
    l_srch_idx = [i for i, col in enumerate(columns) if "search" in str(col).lower()]
    l_anon_idx = [i for i, col in enumerate(columns) if any(x in str(col).lower() for x in ("status", "folder", "path"))]
    l_raw_idx = list(range(len(columns)))  # All columns

    l_job = []

    for row in df.itertuples(index=False):
        row_values = list(row)

        # Search
        s_d = [
            {columns[i].split('.')[0].split('_')[1]: row_values[i]}
            for i in l_srch_idx
        ]
        # Push/Anonymization
        a_d = [
            {columns[i]: row_values[i]}
            for i in l_anon_idx
        ]
        # Raw
        raw_d = [
            {columns[i]: row_values[i]}
            for i in l_raw_idx
        ]
        reversed_dict = dict(reversed(dict(ChainMap(*raw_d)).items()))

        l_job.append({
            "search": dict(ChainMap(*s_d)),
            "push": dict(ChainMap(*a_d)),
            "raw": reversed_dict
        })

    return l_job


