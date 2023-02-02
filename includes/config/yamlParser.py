import yaml

# file = '/Users/igale/vsCode/airflow/includes/config/default.yml'

def Parser(file):
    with open(file, 'r') as yamlfile:
        yamlLoader = yaml.load(yamlfile, Loader=yaml.FullLoader)
        yamlDictionary={}
        for tab in yamlLoader:
            conf = yamlLoader[tab]
            for key in conf:
                # print(k, ' -- ', conf[k])
                yamlDictionary[key] = conf[key]

    return yamlDictionary
