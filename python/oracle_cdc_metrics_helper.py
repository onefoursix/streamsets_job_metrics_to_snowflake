"""
The StreamSets SDK does not currently provide a way to get Oracle CDC lag metrics,
so we'll use the Python requests module to call SDC's REST API directly

"""
import requests


class OracleCDCMetricsHelper:
    def __init__(self, cred_id, cred_token, sch):
        self.cred_id = cred_id
        self.cred_token = cred_token
        self.sch = sch

        # An HTTP session to use to get Oracle CDC lag time metric
        self.session = requests.Session()

    # Get the SDC URL for an SDC ID
    def get_sdc_url_for_id(self, sdc_id):
        for sdc in self.sch.data_collectors:
            if sdc.id == sdc_id:
                return sdc.engine_url
        return None

    # This method returns either a 1 element map like this:
    #      {'ORACLE_CDC_LAG_TIME_SECONDS': <num_seconds>}
    # or a 1 element map like this:
    #      {'ORACLE_CDC_SERVER_INSTANCE_LATENCY': <a time string like 'n minutes m seconds'>}
    # depending on if the origin is the old Oracle CDC Client Origin or the new Oracle CDC Origin, respectively.
    # Returns None if no CDC lag or latency metric is found
    def get_oracle_cdc_lag_time(self, job, job_run):
        # noinspection PyProtectedMember
        sdc_id = job._data['currentJobStatus']['sdcIds'][0]
        sdc_url = self.get_sdc_url_for_id(sdc_id)
        print(' - SDC URL: {}'.format(sdc_url))
        pipeline_id = job_run.engine_pipeline_id
        cdc_metrics_url = '{}/rest/v1/pipeline/{}/metrics?rev=0'.format(sdc_url, pipeline_id)
        self.session.headers.update({'Content-Type': 'application/json',
                                     'X-Requested-By': 'SDC',
                                     'X-SS-Rest-Call': 'true',
                                     'X-SS-App-Component-Id': self.cred_id,
                                     'X-SS-App-Auth-Token': self.cred_token})
        try:
            result = self.session.get(cdc_metrics_url)
            if result.status_code == 200:
                cdc_metrics = result.json()
                for key in cdc_metrics['gauges'].keys():

                    # Old Oracle CDC Client Origin
                    if 'RedoLog Archives' in key:
                        cdc_lag_seconds = cdc_metrics['gauges'][key]['value']['Read lag (seconds)']
                        print(' - Oracle CDC Read lag (seconds) = {}'.format(cdc_lag_seconds))
                        return { 'ORACLE_CDC_LAG_TIME_SECONDS': int(cdc_lag_seconds) }

                    # New Oracle CDC Origin
                    elif 'Summary 02 - Latency.0.gauge' in key:
                        cdc_lag_seconds = cdc_metrics['gauges'][key]['value']['Server Instant Latency']
                        print(' - Oracle CDC Server Instant Latency = {}'.format(cdc_lag_seconds))
                        # We need to return this as a String because some values are a String like "n minutes m seconds"
                        return { 'ORACLE_CDC_SERVER_INSTANCE_LATENCY': str(cdc_lag_seconds) }

            else:
                print('Error getting Oracle CDC metrics for the Job \'{}\'. Received HTTP status code: {}'
                      .format(job.job_name, result.status_code))
        except Exception as e:
            print('Error getting Oracle CDC metrics for the Job \'{}\': {}'
                  .format(job.job_name, str(e)))
            return None
        return None
