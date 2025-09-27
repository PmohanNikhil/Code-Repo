import requests
import pandas as pd

filename = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0101EN-SkillsNetwork/labs/Module%204/data/example1.txt"

def download(url, filename):
    response = requests.get(url)
    print(response.status_code,response.reason)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
            

download(filename, "example1.txt")

print("done")