import requests


response = requests.get("https://developer.trimet.org/ws/V1/arrivals/locIDs/6849,6850/appID/324C58C9AAA75511C1422AA3E")
print(response)
print(response.text)

# Servicecalls
'''
First using slashes to separate the service parameters:
"https://developer.trimet.org/ws/V1/arrivals/locIDs/6849,6850/appID/324C58C9AAA75511C1422AA3E"
Second, using the HTTP GET parameters style:
"https://developer.trimet.org/ws/V1/arrivals?locIDs=6849,6850&appID=324C58C9AAA75511C1422AA3E"
'''
