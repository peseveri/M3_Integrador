import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
SECRET = os.getenv("CLIENT_SECRET")

def get_access_token():
    url = f"{BASE_URL}/applications/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": SECRET,
        "grant_type": "client_credentials"
    }
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_connections(access_token):
    url = f"{BASE_URL}/connections"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    connections = resp.json()["data"]
    return [c["connectionId"] for c in connections]

def trigger_job(access_token,connection_id):
    url = f"{BASE_URL}/jobs"
    payload = {
        "jobType": "sync",
        "connectionId": f"{connection_id}"
    }
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    resp = requests.post(url, json=payload, headers=headers)
    return resp.json()["jobId"]

def get_status(token,job_id):
    url = f"{BASE_URL}/jobs/{job_id}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    return resp.json()["status"]

def main():
    token = get_access_token()
    connection_ids = get_connections(token)

    job_ids = []
    for cid in connection_ids:
        job_id = trigger_job(token, cid)
        job_ids.append(job_id)

    print("⏱ Esperando 4 minutos antes de verificar el estado de todos los jobs...")
    time.sleep(4 * 60)
    
    for job_id in job_ids:
        status = get_status(token, job_id)
        print(f"Estado del job {job_id}: {status}")

        if status == "succeeded":
            print(f"✅ Job {job_id} finalizó con éxito.")
        else:
            print(f"❌ Job {job_id} terminó con estado: {status}")
            raise SystemExit(1)

    print("🎉 Todos los jobs finalizaron correctamente.")

if __name__ == "__main__":
    main()
