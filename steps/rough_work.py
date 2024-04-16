import requests
import json
import os

api_string = 'http://api.stackexchange.com/2.3/answers?order=desc&sort=activity&site=stackoverflow'

response = requests.get(api_string)


# print(response.json()['items'])

for item in response.json()['items']:
    print(item['content_license'])
