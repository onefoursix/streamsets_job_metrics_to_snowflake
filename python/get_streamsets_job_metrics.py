"""
get_streamsets_job_metrics.py

This script retrieves StreamSets Job history and metrics from StreamSets Platform and
writes the results to a local file.  The script can be run standalone from the command line
or can be called by a StreamSets pipeline's start-event, with the pipeline then merging the
retrieved metrics into a Snowflake table.  See the README.md for details.

Prerequisites
-------------
- Python 3.6+ (tested with Python 3.11.5)

- StreamSets API Credentials set in the environment prior to running the script.
  For example:
    $ export CRED_ID="zzzz"
    $ export CRED_TOKEN="zzzz"

- The StreamSets SDK for Python v6.0+ (tested with v6.3)


Command Line Arguments
----------------------
The script requires two command line arguments:

- job_metrics_file - an absolute path to a file to write the metrics to. If the file
  already exists, for example, from a prior run, the file will be overwritten.

- lookback_minutes - How many minutes prior to the current time the script should retrieve
  Job metrics for.


Example Usage
-------------
$ python3 get_streamsets_job_metrics.py /tmp/streamsets_job_metrics.json 60

"""

import os
from datetime import datetime
from time import time
import sys
from streamsets.sdk import ControlHub
import json
from oracle_cdc_metrics_helper import OracleCDCMetricsHelper

# Get Control Hub Credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')

# A Job tag that identifies Oracle CDC Jobs
oracle_cdc_job_tag = 'oracle_cdc'


def print_usage_and_exit():
    print('Usage: $ python3 get_streamsets_job_metrics.py <job_metrics_file> <lookback_minutes>')
    print('Usage Example: $ python3 get_streamsets_job_metrics.py /tmp/streamsets_job_metrics.json 60')
    sys.exit(1)


def convert_timestamp_seconds_to_datetime_string(timestamp_seconds):
    return datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d %H:%M:%S")


# Get metrics for a specific Job run
def get_run_metrics(job_name, run_count, the_metrics):
    for m in the_metrics:
        if m.run_count == job_run.run_count:
            return m
    print('Error finding metrics for run #{} for Job {}'.format(run_count, job_name))
    return None


def is_oracle_cdc_job(the_job):
    for tag in the_job.job_tags:
        if tag['tag'] == oracle_cdc_job_tag:
            return True
    return False


# Check the number of command line args
if len(sys.argv) != 3:
    print('Error: Wrong number of arguments')
    print_usage_and_exit()

# Validate the lookback_minutes command line arg
lookback_minutes = sys.argv[2]
try:
    lookback_minutes = int(lookback_minutes)
except ValueError as ve:
    print('Error: lookback_minutes arg \'{}\' is not an integer'.format(lookback_minutes))
    print_usage_and_exit()

# Get the job metrics file name
job_metrics_file = sys.argv[1]

with (open(job_metrics_file, "w", encoding='utf-8') as output_file):
    # Get the current time
    current_time_seconds = time()

    # Starting time to look for Jobs
    start_time_seconds = int(current_time_seconds - (lookback_minutes * 60))
    start_time_millis = start_time_seconds * 1000

    # Print the settings
    print('-------------------------------------')
    print('Current time is {}'
          .format(convert_timestamp_seconds_to_datetime_string(current_time_seconds)))
    print('Lookback minutes is {}'.format(lookback_minutes))
    print('Will get metrics for Jobs started after {}'
          .format(convert_timestamp_seconds_to_datetime_string(start_time_seconds)))
    print('Metrics will be written to the file {}'.format(job_metrics_file))
    print('-------------------------------------')

    # Connect to Control Hub
    sch = None
    try:
        sch = ControlHub(
            credential_id=cred_id,
            token=cred_token)
    except Exception as e:
        print('Error connecting to Control Hub')
        print(str(e))
        sys.exit(1)
    print('Connected to Control Hub')
    print('-------------------------------------')

    # Create an instance of the OracleCDCMetricsHelper
    cdc_metrics = OracleCDCMetricsHelper(cred_id, cred_token, sch)

    # Job runs to get metrics for
    job_runs = []
    
    # Loop through all Jobs
    for job in sch.jobs:

        # Ignore Job Templates
        if not job.job_template:

            # Get the Job History
            history = job.history

            try:

                # Get the Job Metrics
                metrics = job.metrics

                # Loop through every Job Run for the Job, starting with the most recent
                for job_run in history:

                    # If this Job Run was started or ended within the lookback period or is still ACTIVE
                    if (job_run.start_time >= start_time_millis
                            or job_run.finish_time >= start_time_millis
                            or job_run.status == 'ACTIVE'):

                        # Get the Job Run's metrics
                        run = {}
                        run['ID'] = job.job_id
                        run['NAME'] = job.job_name
                        run['CREATETIME'] = job.created_on
                        run['LASTMODIFIEDON'] = job.last_modified_on
                        run['PIPELINENAME'] = job.pipeline_name
                        run['PIPELINECOMMITLABEL'] = job.commit_label
                        run['RUNCOUNT'] = job_run.run_count
                        run['STARTTIME'] = job_run.start_time
                        run['FINISHTIME'] = job_run.finish_time
                        run['ERRORMESSAGE'] = job_run.error_message
                        run['COLOR'] = job_run.color
                        run['STATUS'] = job_run.status
                        run_metrics = get_run_metrics(job.job_name, job_run.run_count, metrics)

                        # If no metrics exists, set all row counts to -1 as a flag
                        if run_metrics is not None:
                            run['INPUTRECORDS'] = run_metrics.input_count
                            run['OUTPUTRECORDS'] = run_metrics.output_count
                            run['ERRORRECORDS'] = run_metrics.error_count
                        else:
                            run['INPUTRECORDS'] = -1
                            run['OUTPUTRECORDS'] = -1
                            run['ERRORRECORDS'] = -1

                        # Get latency metric for Oracle CDC Jobs
                        if job_run.status == 'ACTIVE' and is_oracle_cdc_job(job):
                            print('Getting CDC latency metric for Oracle CDC Job \'{}\''.format(job.job_name))
                            oracle_cdc_lag_time_metric_map = cdc_metrics.get_oracle_cdc_lag_time(job, job_run)

                            # Unpack the key/value of the Oracle CDC latency metric
                            if oracle_cdc_lag_time_metric_map is not None:
                                for key in oracle_cdc_lag_time_metric_map.keys():
                                    run[key] = oracle_cdc_lag_time_metric_map[key]

                        job_runs.append(run)
                    else:
                        # We're finished with this Job
                        break

            except KeyError as ke:
                print('-------------------------------------')
                print('KeyError Exception getting metrics for Job \'{}\''.format(job.job_name))
                print('Metrics collection for this Job will be skipped')
                print('Exception is: ' + str(e))
                print('Metrics object is of type: ' + str(type(metrics)))
                print('-------------------------------------')

    print('-------------------------------------')
    print('Found {} Job Runs within lookback window'.format(len(job_runs)))

    if len(job_runs) > 0:
        print('Writing Metrics')
        for run in job_runs:
            output_file.write(json.dumps(run) + '\n')

    print('-------------------------------------')
    print('Done')
