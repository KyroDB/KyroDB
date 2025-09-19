#!/usr/bin/env python3

import requests
import json

# Test the batch lookup endpoint
url = "http://127.0.0.1:3030/v1/lookup_batch"
data = [123, 456, 789]

print(f"Testing batch lookup with data: {data}")

try:
    response = requests.post(url, json=data)
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    print(f"Content: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Parsed JSON: {result}")
    else:
        print(f"Error: {response.text}")
        
except Exception as e:
    print(f"Exception: {e}")
