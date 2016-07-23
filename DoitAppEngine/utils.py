
# import httplib

# from oauth2client.client import GoogleCredentials
# from apiclient import discovery
# from oauth2client import client as oauth2client

import base64
import logging
import httplib2
import json
import time
import io
from googleapiclient import discovery
from googleapiclient import errors
from oauth2client import client as oauth2client
import googleapiclient


##########GLOBALS###########
config = json.load(open("config.json"))

pubsub_scope = config["pubsub_scopes"]
bigquery_scope = config["bigquery_scope"]
topic_name = config["topic_name"]
project_id = config["project_id"]
subscriber = 'https://'+project_id+'.appspot.com/subscriber'
subscription_name = config["subscription_name"]

configuration= config["configuration"]

pub_sub_client = None
big_query_client = None

##########################

def get_client():

    global pub_sub_client

    if pub_sub_client is None:
        pub_sub_client = create_pubsub_client()
    return pub_sub_client

def create_pubsub_client(http = None):

    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(pubsub_scope)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return discovery.build('pubsub', 'v1', http=http)

def checkTopicExist(myTopic):

    client = get_client()
    try:
        topic = 'projects/'+project_id+'/topics/'+myTopic+''
        resp = client.projects().topics().get(topic=topic).execute()
        return True
    except errors.HttpError:
        return False


def createTopic(topicName = None):

    if topicName is None:
        topicName = topic_name
    client = get_client()

    #check if Topic exist
    if not checkTopicExist(topicName):
        #create new Topic
        topic = client.projects().topics().create(
        name='projects/' + project_id + '/topics/' + topicName + '', body={}).execute()
        createSubsricption()

def publishToPubSub(msg):

    client = get_client()
    data = base64.b64encode(msg)
    body = {
    'messages': [
        {'data': data}
        ]
    }
    createTopic(topic_name)
    resp = client.projects().topics().publish(
    topic='projects/' + project_id + '/topics/' + topic_name + '', body=body).execute()
    return resp['messageIds']

def createSubsricption(subName = None):

    if subName is None:
        subName = subscription_name
    client = create_pubsub_client()
    push_endpoint = subscriber
    # Create a POST body for the Pub/Sub request
    body = {
        'topic': 'projects/' + project_id + '/topics/' + topic_name + '',
        'pushConfig': {
            'pushEndpoint': push_endpoint
        }
    }

    subscription = client.projects().subscriptions().create(
    name='projects/' + project_id + '/subscriptions/' + subName + '',
    body=body).execute()
    logging.info('Created: %s' % subscription.get('name'))

def get_big_query():

    global big_query_client

    if big_query_client is None:
        big_query_client = create_big_query_client()
    return big_query_client

def create_big_query_client(http = None):

    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(bigquery_scope)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)
    V2_DISCOVERY_URI = 'https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest'
    return discovery.build('bigquery', 'v2', http=http, discoveryServiceUrl=V2_DISCOVERY_URI)

def storeMsgToBiqQuery(msg):

    payload = json.dumps(msg['message']['data'])
    body_str = base64.b64decode(payload)
    if body_str == "":
        return 400
    with open('data.json', 'w+') as f:
        json.dump(body_str, f)
        f.close()

    bigQuery = get_big_query()
    insert_request = bigQuery.jobs().insert(
        projectId = project_id,
        body = {
            'configuration': configuration
        },
        media_body = googleapiclient.http.MediaFileUpload('data.json', 'application/octet-stream'))
    job = insert_request.execute()
    status_request = bigQuery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    # Poll the job until it finishes.
    while True:
        result = status_request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if result['status'].get('errors'):
                return 400
            else:
                return 200

        time.sleep(1)