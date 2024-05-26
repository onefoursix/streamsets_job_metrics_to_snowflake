## StreamSets Job Metrics to Snowflake
This project provides an example of how to replicate StreamSets Job Metrics to Snowflake.  

This allows operators to easily run SQL queries and build custom dashboards on top of StreamSets Job metrics and history. For example, one could display a list of currently running Jobs with their record counts, or compare historical record counts and performance metrics of the same Job run every day for the past month.

*Important note: This example uses a [Shell Executor](https://docs.streamsets.com/portal/platform-datacollector/latest/datacollector/UserGuide/Executors/Shell.html#concept_jsr_zpw_tz).  For production use, make sure to configure [Data Collector Shell Impersonation Mode](https://docs.streamsets.com/portal/platform-datacollector/latest/datacollector/UserGuide/Executors/Shell.html#concept_n2w_txv_vz).*

### Design Approach
There are several ways one could implement this functionality:

- A StreamSets pipeline could use the StreamSets REST API to get Job history and metrics and merge the data into a Snowflake table. The advantages of this approach include the use of a single pipeline without needing a Shell Executor, a Snowflake connector with merge and data-drift support, scheduling, monitoring and failover; however, getting and filtering Job history and metrics using the REST API is somewhat complex.

- A [StreamSets SDK for Python](https://docs.streamsets.com/platform-sdk/latest/welcome/overview.html) script could perform the same actions. The advantages of this approach include an elegant and convenient syntax for getting Job history and metrics, but would require custom code to merge the data into Snowflake, and has no built in scheduling, monitoring and failover.

- A hybrid approach! This example uses a StreamSets SDK script within a pipeline to get the best of both worlds: an elegant retrieval of Job history and metrics using the SDK, and all of the operational benefits of using a StreamSets pipeline including scheduling, monitoring and failover. 

The example pipeline allows the user to set a <code>lookback</code> period that determines how far back in time to get Job history and metrics for, for example 5 minutes, 1 hour, 24 hours, 30 days, etc...  

The script is idempotent, so that running it multiple times will not result in duplicate data in Snowflake. For Jobs that are actively running across two or more executions of the script, the Job run's metrics will be updated.

A Job could be created for this pipeline, and that Job could be scheduled to run as often as necessary, for example, to keep a dashboard updated every five minutes.



### Prerequisites

- A Python 3.6+ environment with the StreamSets Platform SDK v6.0+ module installed. This example was tested using Python 3.11.5 and StreamSets SDK v6.3.

- StreamSets [API Credentials](https://docs.streamsets.com/portal/platform-controlhub/controlhub/UserGuide/OrganizationSecurity/APICredentials_title.html#concept_vpm_p32_qqb)

## Deploying the Example

### Test the SDK Script
- Copy the file [get_streamsets_job_metrics.py](python/get_streamsets_job_metrics.py) to a location on the Data Collector machine. I'll copy mine to  <code>/home/mark/scripts/get_streamsets_job_metrics.py</code>

- Export your StreamSets Platform API Credentials:
```
	$ export CRED_ID="..."
	$ export CRED_TOKEN="..."
```

- Execute the script passing it the name of a metrics file to be written and the number of lookback minutes:

```
	$ python3 get_streamsets_job_metrics.py /tmp/streamsets_job_metrics.json 60
```

You should see output like this:

```
	$ python3 get_streamsets_job_metrics.py /tmp/streamsets_job_metrics.json 60
	-------------------------------------
	Current time is 2024-05-26 17:49:23
	Lookback minutes is 60
	Will get metrics for Jobs started after 2024-05-26 16:49:23
	Metrics will be written to the file /tmp/streamsets_job_metrics.json
	-------------------------------------
	Connected to Control Hub
	-------------------------------------
	Found 7 Job Runs within lookback window
	Writing Metrics
	-------------------------------------
	Done
```

Inspect the metrics file written by the script. It should look like this:

```
	$ cat /tmp/streamsets_job_metrics.json
	{"ID": "208e479f-c34c-4379-8529-c03d5c6d3f60:8030c2e9-1a39-11ec-a5fe-97c8d4369386", "NAME": "Get Weather Events", "CREATETIME": 1696984398077, "LASTMODIFIEDON": 1716475664195, "PIPELINENAME": "Get Weather Events", "PIPELINECOMMITLABEL": "v39", "RUNCOUNT": 110, "STARTTIME": 1716745144841, "FINISHTIME": 0, "ERRORMESSAGE": null, "COLOR": "GREEN", "STATUS": "ACTIVE", "INPUTRECORDS": 499, "OUTPUTRECORDS": 499, "ERRORRECORDS": 0}
	{"ID": "fe9605ab-4912-4181-a315-e49d031a0d50:8030c2e9-1a39-11ec-a5fe-97c8d4369386", "NAME": "Oracle to Snowflake Bulk Load", "CREATETIME": 1716561746294, "LASTMODIFIEDON": 1716562027770, "PIPELINENAME": "Oracle to Snowflake Bulk Load", "PIPELINECOMMITLABEL": "v9", "RUNCOUNT": 12, "STARTTIME": 1716745294569, "FINISHTIME": 1716745325738, "ERRORMESSAGE": null, "COLOR": "GRAY", "STATUS": "INACTIVE", "INPUTRECORDS": 26079, "OUTPUTRECORDS": 26082, "ERRORRECORDS": 0}
	{"ID": "de4a50a5-7f81-4f55-8dd7-1fc8614c2148:8030c2e9-1a39-11ec-a5fe-97c8d4369386", "NAME": "Weather Raw to Refined", "CREATETIME": 1716330812999, "LASTMODIFIEDON": 1716475898652, "PIPELINENAME": "Weather Raw to Refined", "PIPELINECOMMITLABEL": "v26", "RUNCOUNT": 7, "STARTTIME": 1716745145236, "FINISHTIME": 0, "ERRORMESSAGE": null, "COLOR": "GREEN", "STATUS": "ACTIVE", "INPUTRECORDS": 497, "OUTPUTRECORDS": 497, "ERRORRECORDS": 0}
	...
```

--> If you have made it this far. the SDK script is working!

### Import and Configure the pipeline
Import the pipeline from the archive file [pipelines/Job_History_to_Snowflake.zip](pipelines/Job_History_to_Snowflake.zip)

The pipeline looks like this:
<img src="images/pipeline.png" alt="pipeline" width="700"/>

