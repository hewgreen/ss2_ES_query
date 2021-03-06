import hca.dss
from hca.dss import DSSClient
import requests
import json
import sys
from tqdm import tqdm

def bundle_url_iterator():

    dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "files.library_preparation_protocol_json.library_construction_approach.ontology": "EFO:0008931"
                        }
                    },
                    {
                        "match": {
                            "files.sequencing_protocol_json.paired_end": 'true'
                        }
                    },
                    {
                        "match": {
                            "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": 9606
                        }
                    }
                ],
                "should": [
                    {
                        "match": {
                            "files.dissociation_protocol_json.dissociation_method.ontology": "EFO:0009128"
                        }
                    },
                    {
                        "match": {
                            "files.dissociation_protocol_json.dissociation_method.text": "mouth pipette"
                        }
                    }
                ],
                "must_not": [
                    {
                        "terms": {
                            "files.project_json.provenance.document_id": [
                                "1630e3dc-5501-4faf-9726-2e2c0b4da6d7",
                                "fd1d163d-d6a7-41cd-b3bc-9d77ba9a36fe",
                                "2a0faf83-e342-4b1c-bb9b-cf1d1147f3bb",
                                "cf8439db-fcc9-44a8-b66f-8ffbf729bffa",
                                "6b9f514d-d738-403f-a9c2-62580bbe5c83",
                                "311d013c-01e4-42c0-9c2d-25472afa9cbc",
                                "d237ed6a-3a7f-4a91-b300-b070888a8542",
                                "e6cc0b02-2125-4faa-9903-a9025a62efec",
                                "e4dbcb98-0562-4071-8bea-5e8de5f3c147",
                                "e79e9284-c337-4dfd-853d-66fa3facfbbd",
                                "560cd061-9165-4699-bc6e-8253e164c079",
                                "e83fda0e-6515-4f13-82cb-a5860ecfc2d4",
                                "9a60e8c2-32ea-4586-bc1f-7ee58f462b07",
                                "71a6e049-4846-4c2a-8823-cc193c573efc",
                                "4b5a2268-507c-46e6-bab0-3efb30145e85",
                                "364ebb73-652e-4d32-8938-1c922d0b2584",
                                "11f5d59b-0e2c-4f01-85ac-8d8dd3db53be",
                                "c1996526-6466-40ff-820f-dad4d63492ec",
                                "c281dedc-e838-4464-bf51-1cc4efae3fb9",
                                "40afcf6b-422a-47ba-ba7a-33678c949b5c",
                                "71a6e049-4846-4c2a-8823-cc193c573efc",
                                "9a60e8c2-32ea-4586-bc1f-7ee58f462b07",
                                "0facfacd-5b0c-4228-8be5-37aa1f3a269d",
                                "76c209df-42bf-41dc-a5f5-3d27193ca7a6",
                                "bb409c34-bb87-4ed2-adaf-6d1ef10610b5",
                                "1a6b5e5d-914f-4dd6-8817-a1f9b7f364d5",
                                "dd401943-1059-4b2d-b187-7a9e11822f95"
                            ]
                        }
                    }
                ]
            }
        }
    }


    return dss_client.post_search.iterate(replica="aws", es_query=q) #iterator of bundles

def bundle_metadata_analysis(bundle_url):
    # querystring = {"version": "2019-02-01T095105.528278Z", "replica": "aws"}
    response = requests.request("GET", bundle_url)
    json_data = json.loads(response.text)
    files = json_data.get('bundle').get('files')

    for file in files:
        filename = file.get('name')
        if filename == 'project_0.json':
            return file.get('uuid')



if __name__ == '__main__':
    
    projects = []
    hits = 23749 # known here from extra search
    # bundle_url = 'https://dss.data.humancellatlas.org/v1/bundles/ffffa79b-99fe-461c-afa1-240cbc54d071?version=2019-02-27T223320.296197Z&replica=aws'
    for ref in tqdm(bundle_url_iterator(), total=23749):
        print(ref)
        sys.exit()
        bundle_url = ref.get('bundle_url')
        project_uuid = bundle_metadata_analysis(bundle_url)
        if project_uuid not in projects:
            projects.append(project_uuid)
    print(projects)

























