#! /usr/bin/python3

import requests
from datetime import date
import sys
import os

def main(args):

    username = os.getlogin()

    response = requests.get("https://developer.trimet.org/ws/V1/arrivals/locIDs/6849,6850/appID/324C58C9AAA75511C1422AA3E")

    filename = "/home/" + username + "/InforMetro/trimet_data/" + date.today().strftime('%m-%d-%Y') + ".txt"
    #filename = "./trimet_data/" + date.today().strftime('%m-%d-%Y') + ".txt"
    f = open(filename, "w")
    f.write(response.text)

if __name__ == '__main__':
    main(sys.argv)


# Servicecalls
'''
First using slashes to separate the service parameters:
"https://developer.trimet.org/ws/V1/arrivals/locIDs/6849,6850/appID/324C58C9AAA75511C1422AA3E"
Second, using the HTTP GET parameters style:
"https://developer.trimet.org/ws/V1/arrivals?locIDs=6849,6850&appID=324C58C9AAA75511C1422AA3E"
'''
