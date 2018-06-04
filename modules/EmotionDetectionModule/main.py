# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import os
import random
import time
import sys
import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from io import BytesIO
import requests, json, datetime, uuid
from PIL import Image
from random import randint
import requests, uuid, datetime

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

# Choose HTTP, AMQP or MQTT as transport protocol.  Currently only MQTT is supported.
PROTOCOL = IoTHubTransportProvider.MQTT

# String containing Hostname, Device Id & Device Key & Module Id in the format:
# "HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>;ModuleId=<module_id>;GatewayHostName=<gateway>"
CONNECTION_STRING = "[Device Connection String]"

hub_manager = None
img_path = 'C:\\_ccc\\images'
url = "https://eastus.api.cognitive.microsoft.com/face/v1.0/detect?returnFaceId=true&returnFaceLandmarks=false&returnFaceAttributes=emotion,gender,age"
subscription_key = "51ad9771bb0c4058a593e18de83b5fff"
header = {'Ocp-Apim-Subscription-Key': subscription_key, 'Content-Type': 'application/octet-stream' }

images = list(os.scandir(path=img_path))

def device_message_callback(message, result, user_context):
    print("device message callback")

def get_image_analysis(img_data):
    response = requests.post(url, headers=header, data=img_data)
    response.raise_for_status()
    analysis = response.json()
    return analysis

def get_image():
    idx = randint(0, len(images)-1)
    img_filename = images[idx].path
    img_data = open(img_filename, "rb").read()
    return img_data

def construct_analysis_document(analysis):
    doc = {'id': str(uuid.uuid4()), 'type': 'face_analysis' 'datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), 'faces': analysis}
    return doc

def image_generator_timer():
    img_arr = get_image()
    analysis = get_image_analysis(img_arr)
    doc_to_upload = construct_analysis_document(analysis)
    hub_manager.send_analysis_to_output(doc_to_upload)
    time.sleep(5)
    image_generator_timer()

class HubManager(object):

    def __init__(
            self,
            connection_string):
        self.client_protocol = PROTOCOL
        self.client = IoTHubClient(connection_string, PROTOCOL)

        # set the time until a message times out
        self.client.set_option("messageTimeout", MESSAGE_TIMEOUT)
        # some embedded platforms need certificate information
        self.set_certificates()

    def set_certificates(self):
        isWindows = sys.platform.lower() in ['windows', 'win32']
        if not isWindows:
            CERT_FILE = os.environ['EdgeModuleCACertificateFile']        
            print("Adding TrustedCerts from: {0}".format(CERT_FILE))
            
            # this brings in x509 privateKey and certificate
            file = open(CERT_FILE)
            try:
                self.client.set_option("TrustedCerts", file.read())
                print ( "set_option TrustedCerts successful" )
            except IoTHubClientError as iothub_client_error:
                print ( "set_option TrustedCerts failed (%s)" % iothub_client_error )

            file.close()

    # Forwards the message received onto the next stage in the process.
    def send_analysis_to_output(self, msg):
        msg_txt = json.dumps(msg)
        hubmessage = IoTHubMessage(bytearray(msg_txt, 'utf8'))
        self.client.send_event_async("output1", hubmessage, device_message_callback, 0)
        # self.client.send_event_async(
        #     "output1", event, send_confirmation_callback, send_context)

def main(connection_string):
    global hub_manager
    try:
        print ( "\nPython %s\n" % sys.version )
        print ( "IoT Hub Client for Python" )

        hub_manager = HubManager(connection_string)


        print ( "Starting the IoT Hub Python sample using protocol %s..." % hub_manager.client_protocol )
        print ( "The sample is now waiting for messages and will indefinitely.  Press Ctrl-C to exit. ")

        image_generator_timer()

        while True:
            time.sleep(1000)

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return

    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

if __name__ == '__main__':
    try:
        CONNECTION_STRING = os.environ['EdgeHubConnectionString']

    except Exception as error:
        print ( error )
        sys.exit(1)

    main(CONNECTION_STRING)