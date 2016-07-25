
import utils

# [START app]
import logging
import base64
import json
from flask import Flask
from flask import request
import jsonschema

app = Flask(__name__)
schema = json.load(open("event_schema.json"))

@app.route('/', methods=['POST'])
def handlePostFromMobileApp():
     try:
        json_body = json.loads(request.data) #.decode('utf-8')
        jsonschema.validate(json_body, schema)
        if utils.publishToPubSub(request.data) is not None:
            return 'OK', 200
        else:
            return 'Invalid request', 400
     except jsonschema.exceptions.ValidationError:
        return 'Invalid request', 400

@app.route('/subscriber', methods=['POST'])
def handlePostFromPubSub():
    response = utils.storeMsgToBiqQuery(json.loads(request.data.decode('utf-8')))

    if not response["insertErrors"]:
        return 'Invalid request', 400
    return 'OK', 200

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]

