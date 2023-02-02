import yaml
from includes.config.conf import ROOT_PATH

conf_file = f"{ROOT_PATH}/includes/config/default.yml"

def Parser(file=conf_file):
    with open(file, 'r') as yamlfile:
        yamlLoader = yaml.load(yamlfile, Loader=yaml.FullLoader)
        yamlDictionary={}
        for tab in yamlLoader:
            conf = yamlLoader[tab]
            for key in conf:
                # print(k, ' -- ', conf[k])
                yamlDictionary[key] = conf[key]

    return yamlDictionary

if __name__=="__main__":
    print('----', Parser())