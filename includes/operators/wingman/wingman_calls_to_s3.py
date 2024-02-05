from airflow.models import BaseOperator
import os
import tempfile
from datetime import date
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
import json
import time
from typing import Optional


class WingmanCallsToS3Operator(BaseOperator):
    """
        An Airflow Operator to retrieve Wingman calls data and upload it to Amazon S3.
        :param wingman_key: The API key for authenticating with the Wingman API.
        :type wingman_key: str
        :param wingman_password: The API password for authenticating with the Wingman API.
        :type wingman_password: str
        :param wingman_conn: The name of the Airflow connection to Wingman.
        :type wingman_conn: str
        :param skip: The number of calls to skip in the API response pagination. Default is 0.
        :type skip: int
        :param limit: The maximum number of calls to fetch per API request. Default is 100.
        :type limit: int
        :param min_duration_filter: The minimum call duration (in seconds) to filter calls. Default is 1.
        :type min_duration_filter: int
        :param call_start_date_filter: The start date to filter calls (ISO date-time format, e.g., '2023-09-01T00:00:00Z').
        :type call_start_date_filter: str
        :param call_end_date_filter: The end date to filter calls (ISO date-time format, e.g., '2023-09-30T23:59:59Z'). Defaults to None.
        :type call_end_date_filter: str
        :param query_extra_params: Additional query parameters to include in the API request (dictionary).
        :type query_extra_params: dict
        :param s3_bucket_name: The name of the Amazon S3 bucket where the data will be uploaded.
        :type s3_bucket_name: str
        :param s3_key: The key (path) under which the data will be stored in the S3 bucket.
        :type s3_key: str
        :param aws_conn_id: The name of the connection that has the parameters we need to connect to S3.
        :type aws_conn_id: str
        :param replace: A flag to decide whether or not to overwrite the S3 key if it already exists. If set to False and the key exists an error will be raised.
        :type replace: bool
         :param encrypt: If True, the file will be encrypted on the server-side by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        :param gzip: If True, the file will be compressed locally.
        :type gzip: bool
        :param acl_policy: String specifying the canned ACL policy for the file being uploaded to the S3 bucket.
        :type acl_policy: str
        """
    template_fields = ('s3_key', 'call_start_date_filter', 'call_end_date_filter',)
    ui_color = '#6872f7'

    def __init__(
            self,
            *,
            wingman_conn: str = "wingman_default",
            skip: int = 0,
            limit: int = 100,
            min_duration_filter: int = 1,
            call_start_date_filter: str,
            call_end_date_filter: Optional[str] = None,
            query_extra_params: dict = {},
            s3_bucket_name: str,
            s3_key: str,
            aws_conn_id: str = "aws_default",
            replace: bool = True,
            encrypt: bool = False,
            gzip: bool = False,
            acl_policy: Optional[str] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wingman_conn = wingman_conn
        self.skip = skip
        self.limit = limit
        self.min_duration_filter = min_duration_filter
        self.call_start_date_filter = call_start_date_filter
        self.call_end_date_filter = call_end_date_filter
        self.query_extra_params = query_extra_params
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def execute(self, context):
        http_hook = HttpHook(method='GET', http_conn_id=self.wingman_conn)

        headers, full_params = self.prepare_request(http_hook)

        with tempfile.TemporaryDirectory() as temp_dir:

            json_file_name = 'wingman_calls.json'
            json_file_path = os.path.join(temp_dir, json_file_name)

            with open(json_file_path, mode='w') as json_file:
                while True:
                    response = http_hook.run(endpoint='/calls', data=full_params, headers=headers)
                    http_hook.check_response(response)
                    data = response.json()

                    calls = data.get('calls', [])

                    self.log.info(f"Fetched {len(calls)} calls (Total: {self.skip + len(calls)})")

                    for call in calls:
                        json.dump(call, json_file)
                        json_file.write('\n')

                    has_more = data.get('pagination', {}).get('hasMore', False)

                    if has_more:
                        self.skip += self.limit
                        full_params['skip'] = self.skip
                        # delay of 0.1 seconds to comply with rate limit (api-doc.trywingman.com/#section/Rate-limit)
                        time.sleep(0.1)
                    else:
                        break

            self.log.info(f"Uploading Wingman calls data to S3 bucket: {self.s3_bucket_name}, key: {self.s3_key}")

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_file(
                filename=json_file_path,
                key=self.s3_key,
                bucket_name=self.s3_bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )

            s3_uri = f"s3://{self.s3_bucket_name}/{self.s3_key}"
            self.log.info("Wingamn calls uploaded to S3 at %s.", s3_uri)

            return s3_uri

    def prepare_request(self, http_hook):
        headers = {
            'X-Api-Key': http_hook.get_connection(self.wingman_conn).login,
            'X-Api-Password': http_hook.get_connection(self.wingman_conn).get_password()
        }

        query_params = {
            'filterTimeGt': self.call_start_date_filter,
            'filterTimeLt': str(date.today()) if self.call_end_date_filter is None else self.call_end_date_filter,
            'filterDurationGt': self.min_duration_filter,
            'limit': self.limit,
            'skip': self.skip
        }
        full_params = {**query_params, **self.query_extra_params}

        return headers, full_params