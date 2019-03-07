import json
import sys
import hca.dss
from hca.dss import DSSClient
import pandas as pd

projects = ['aabbec1a-1215-43e1-8e42-6489af25c12c', 'e8642221-4c2c-4fd7-b926-a68bce363c88', 'f8880be0-210c-4aa3-9348-f5a423e07421']

project_q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "files.project_json.provenance.document_id": [
                                "PROJECT_UUID"
                            ]
                        }
                    }
                ]
            }
    }
}


dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")

project_info = {}
for uuid in projects:
#     q = str(project_q).replace('PROJECT_UUID', uuid)

    project_q.get('query').get('bool').get('must')[0].get('terms')["files.project_json.provenance.document_id"] = [uuid]
    q = json.loads(json.dumps(project_q)) # this is just to ensure the query is in the correct format
    rel = next(dss_client.post_search.iterate(replica="aws", es_query=q, output_format="raw"))

    project_i = rel.get('metadata').get('files').get('project_json')[0]
    project_uuid = rel.get('metadata').get('files').get('project_json')[0].get('provenance').get('document_id')
    project_short_name = project_i.get('project_core').get('project_short_name')
    project_title = project_i.get('project_core').get('project_title')
    supplementary_links = project_i.get('supplementary_links')
    entry = {'Project short name' : project_short_name, 'Project title': project_title, 'Links' : supplementary_links}
    project_info[project_uuid] = entry
df = pd.DataFrame.from_dict(project_info, orient='index')
print(df)
