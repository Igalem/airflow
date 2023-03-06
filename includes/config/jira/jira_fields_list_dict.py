jira_fields_list = {
    'created': None,
    'updated': None,
    'summary': None,
    'description': None,
    'issuetype': 'name',
    'status': 'name',
    'project': 'name',
    'reporter': 'displayName',
    'creator': 'displayName',
    'assignee': 'displayName',
    'priority': 'name',
    'labels': None,
    'comment': 'total',
    'watches': 'watchCount',
    'votes': 'votes',
    'timeestimate': None,
    'timetracking': 'timeSpentSeconds',
    'progress': 'total',
    'subtasks': 'key',
    'components': 'name'
}

jira_mapping_fields = {
    'created': 'created',
    'updated': 'updated',
    'summary': 'title',
    'description': 'description',
    'issuetype': 'issue_type',
    'status': 'status',
    'project': 'project',
    'reporter': 'reporter',
    'creator': 'creator',
    'assignee': 'assignee',
    'priority': 'priority',
    'labels': 'labels',
    'comment': 'total_comments',
    'watches': 'total_watches',
    'votes': 'total_votes',
    'timeestimate': 'timeestimate',
    'timetracking': 'timetracking',
    'progress': 'progress',
    'subtasks': 'subtasks',
    'components': 'components'
}


def listToString(list_values):
    return ','.join(list_values)


def generate_jira_field_names():
    jiraFieldNames = []
    for i, jfield in enumerate(jira_fields_list):
        try:
            jiraFieldNames.append(f"${i + 2} as {jira_mapping_fields[jfield]}")
        except:
            raise Exception(F"Mapping is missing for Jira field name: [{jfield}]")
            sys.exit(1)


    return listToString(jiraFieldNames)