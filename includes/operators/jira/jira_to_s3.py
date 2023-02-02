from airflow.models import BaseOperator
import os
import tempfile

from typing import Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.atlassian.jira.hooks.jira import JiraHook


class JiraToS3Operator(BaseOperator):
    """
    Submits a Jira query and uploads the results to AWS S3.
    :param jql: The jql query to send to Jira.
    :param s3_bucket_name: The bucket name to upload to.
    :param s3_key: The object name to set when uploading the file.
    :param jira_connection_id: The name of the connection that has the parameters needed
        to connect to Jira.
    :param startAt: Index of the first issue to return. (Default: 0)
    :param maxResults (int): Maximum number of issues to return. Total number of results
            is available in the ``total`` attribute of the returned :class:`~jira.client.ResultList`.
            If maxResults evaluates as False, it will try to get all issues in batches. (Default: 50)
    :param jira_fields: comma-separated string or list of issue fields to include in the results.
            Default is to include all fields.
    :param aws_conn_id: The name of the connection that has the parameters we need to connect to S3.
    :param replace: A flag to decide whether or not to overwrite the S3 key if it already exists. If set to
        False and the key exists an error will be raised.
    :param encrypt: If True, the file will be encrypted on the server-side by S3 and will
        be stored in an encrypted form while at rest in S3.
    :param gzip: If True, the file will be compressed locally.
    :param acl_policy: String specifying the canned ACL policy for the file being uploaded
        to the S3 bucket.
    """

    template_fields = ('s3_key', 'jql',)
    ui_color = '#68b0f7'

    def __init__(
            self,
            *,
            jql: str,
            startAt: int = 0,
            maxResults: int = 100,
            s3_bucket_name: str,
            s3_key: str,
            aws_conn_id: str = "aws_default",
            replace: bool = False,
            encrypt: bool = False,
            gzip: bool = False,
            acl_policy: Optional[str] = None,
            jira_connection_id: str,
            jira_fields: list = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.jql = jql
        self.startAt = startAt
        self.maxResults = maxResults
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy
        self.jira_connection_id = jira_connection_id
        self.jira_fields = jira_fields

    def execute(self, context):
        jira_hook = JiraHook(jira_conn_id=self.jira_connection_id).get_conn()
        fields = [field for field in self.jira_fields]
        self.log.info(f"Jira jql: {self.jql}")

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "jira_temp_file")
            issue_value = []

            with open(path, 'a+') as f:
                while True:
                    issues = jira_hook.search_issues(jql_str=self.jql, startAt=self.startAt, maxResults=self.maxResults, fields=fields, json_result=True)['issues']
                    self.log.info('Jira exporting data is in progress...')
                    if not issues:
                        break
                    for issue in issues:
                        issue_value.append(issue['key'])
                        for field_name in self.jira_fields:
                            # get field key value
                            field_key = self.jira_fields[field_name]
                            # get issue values by field mapping
                            field_values = self.get_field_values(row=issue['fields'], field_name=field_name,
                                                                 field_key=field_key)
                            # get string values from list type
                            issue_value.append(
                                f"'{self.get_str_from_list(field_values=field_values, field_key=field_key)}'")

                        f.write((','.join(issue_value)) + '\n')
                        issue_value = []
                    self.startAt += self.maxResults

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_file(
                filename=path,
                key=self.s3_key,
                bucket_name=self.s3_bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )

            s3_uri = f"s3://{self.s3_bucket_name}/{self.s3_key}"
            self.log.info(f"Jira data uploaded to S3 at {s3_uri}.")

            return s3_uri

    def stringHandler(self, string=None):
        string = '' if string is None else string.replace("'", "")  # .replace('\r\n', '').replace('\n', '')
        return string

    def get_field_values(self, row, field_name, field_key=None):
        try:
            if isinstance(row[field_name], list):
                field_values = row[field_name]
            elif field_key is not None:
                field_values = str(row[field_name][field_key])

            else:
                field_values = str(row[field_name])
        except:
            field_values = ''

        return field_values

    def get_str_from_list(self, field_values, field_key=None):
        try:
            if isinstance(field_values, list):
                field_str_list = [str(value[field_key]) for value in field_values]
                return self.stringHandler(string=','.join(field_str_list))
            else:
                return self.stringHandler(string=field_values)
        except:
            field_str_list = [str(value) for value in field_values]
            return self.stringHandler(string=','.join(field_str_list))