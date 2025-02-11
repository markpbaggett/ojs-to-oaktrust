import requests

key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.IjQzZGVmMjY1MjdjZTQxMTAyNTg0MzFiZjRjMDUxMDg2NWEyNzA2YmMi.RkxD_AwM4BPK6VyMc8yfuufFQXXo_mmFw7UI-C29UaE'
headers = {
    'Authorization': 'Bearer ' + key
}

r = requests.get('https://ciney-ojs-tamu.tdl.org/ciney/api/v1/issues', headers=headers)
print(r.json())