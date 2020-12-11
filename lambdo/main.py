__author__="Alexandr Savinov"

import os
import sys
import argparse

import json
import re

from lambdo.Workflow import *
from lambdo.version import *

import logging
log = logging.getLogger('lambdo')


def run(workflow_file):

    with open(workflow_file, encoding='utf-8') as f:
        wf_str = f.read()

        # Remove everything starting with // till the end of line
        wf_str = re.sub(r"//.*$", "", wf_str, flags=re.M)

        wf_json = json.loads(wf_str)
    wf = Workflow(wf_json)
    wf.execute()

    return 0

def main(args = None):
    if not args: args = sys.argv[1:]

    programVersion = 'Version ' + VERSION
    programDescription = 'Lambdo: Feature engineering and machine learning - together at last! ' + programVersion

    parser = argparse.ArgumentParser(description=programDescription)
    parser.add_argument('-v', '--version', action='version', version=programVersion)

    parser.add_argument('-l', '--log', dest="loglevel", required=False, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help="Set the logging level (default INFO)")

    parser.add_argument('workflow_file', type=str, help='workflow JSON file')

    arguments = parser.parse_args(args)

    # Configure logging
    logging.basicConfig(stream=sys.stderr, level=arguments.loglevel, format='%(asctime)s - %(name)s - %(levelname)s: %(message)s')

    # Environment
    log.info(programDescription)

    exitcode = 1
    try:
        exitcode = run(arguments.workflow_file)
    except Exception as e:
        log.error("Error executing workflow file {}. ".format(arguments.workflow_file))
        log.exception(e)

    logging.shutdown()

    return exitcode

if __name__ == "__main__":

    exitcode = main(sys.argv[1:])

    if(not exitcode):
        exit()
    else:
        exit(exitcode)
