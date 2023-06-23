import requests

url = 'http://localhost:8080/api/v1/dags/my_api_triggered_dag/dagRuns'
headers = {
    'Content-Type': 'application/json'
}
auth = ('admin', 'admin')
data = {
    "conf": {
        "my_favorite_color": "violet"
    }
}

response = requests.post(url, headers=headers, auth=auth, json=data)

if response.status_code == 200:
    print("API call successful.")
    print(response.text)
else:
    print(f"API call failed with status code {response.status_code}.")
    print(response.text)
