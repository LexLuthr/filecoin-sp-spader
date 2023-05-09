import os
import random
import sys
import subprocess
import json
import threading
import time
import jwt

import aria2p as aria2p
import requests
from shutil import which

# TODO: Test

# Logging directory full path (must be owned by current user)
log_directory = "/a/b/c"

# Number of deals/proposals to be handled simultaneously
max_concurrent_proposals = 5

# Miner ID
spid = "fXXXX"

# Download directory full path (must be owned by current user)
download_dir = "/a/b/c"

# Download directory size in GiBs (limits how many deals are processed at once)
dir_size = 500

# Spade authenticator script (fil-spid.bash) location (must be full path)
spade_script = "/a/b/c"

# Command used to run the aria2c daemon
aria2c_daemon = "aira2c --daemon --enable-rpc --rpc-listen-port=6801 --keep-unfinished-download-result"
aria2c_config = " --save-session=" + log_directory + "/aria2c.session --save-session-interval=2"
aria2c_log = " --log=" + log_directory + "/aria2c.log"
aria2c_cmd = aria2c_daemon + aria2c_config + aria2c_log

# Spade URLs
pending_proposals_url = "https://api.spade.storage/sp/pending_proposals"
eligible_proposals_url = "https://api.spade.storage/sp/eligible_pieces"
send_deal_url = "https://api.spade.storage/sp/invoke"


class ThreadPool(object):
    def __init__(self):
        super(ThreadPool, self).__init__()
        self.process = {}
        self.lock = threading.Lock()

    def addthread(self, p):
        with self.lock:
            self.process[p.id] = p.thread

    def removethread(self, i):
        with self.lock:
            self.process.pop(i)

    def checkthread(self, i):
        with self.lock:
            keys = self.process.keys()
            if i in keys:
                return True
            else:
                return False


def setup():
    # Check if aria2c exists
    if which("aria2c") is None:
        print(f"Error: Utility aria2c does not exist")
        sys.exit(1)

    # Check if log directory exists
    if not os.path.exists(log_directory):
        print(f"Error: Download directory {log_directory} does not exist")
        sys.exit(1)

    # Check if download directory exists
    if not os.path.exists(download_dir):
        print(f"Error: Download directory {download_dir} does not exist")
        sys.exit(1)

    # Check if download directory has enough free space TODO: Better calculation here, take occupied space in account
    if os.statvfs(download_dir).f_frsize * os.statvfs(download_dir).f_bavail < dir_size * 1024 * 1024 * 1024:
        print(f"Error: Download directory {download_dir} does not have enough space")
        sys.exit(1)

    # Check if spade script exists
    if not os.path.exists(spade_script):
        print(f"Error: Spade script {spade_script} does not exist")
        sys.exit(1)


def generate_spade_auth(extra=None):
    if extra:
        command = extra + " | " + spade_script + " " + spid
        auth_token = subprocess.check_output([command]).decode().strip()
        return {"Authorization": auth_token}
    else:
        auth_token = subprocess.check_output([spade_script, spid]).decode().strip()
        return {"Authorization": auth_token}


# Generates a list of pending proposals for the miner from Spade API
# List is then sorted based on time remaining to seal the deal
def generate_pending_proposals():
    auth_header = generate_spade_auth()

    response = requests.get(pending_proposals_url, headers=auth_header)
    pending_proposals = []

    if response.status_code == 200:
        data = response.json()
        print(data)
        print("INFO: Generating a list of pending proposals")
        for item in data['response']['pending_proposals']:
            pending_proposals.append(item)

        if len(pending_proposals) > 0:
            # Sort and return the pending proposal based on remaining time
            sp = sorted(pending_proposals, key=lambda d: d['hours_remaining'])
            print("##### Pending Proposals #####")
            for p in sp:
                print(p)
            return sp
        else:
            return pending_proposals
    else:
        print("ERROR: pending proposals request failed with status code:", response.status_code)
        return pending_proposals


# query_deal_status takes deal UUID and piece CID. It returns True or False
# to process the deal
def query_deal_status(deal_uuid, piece_cid):
    # Set up the query payload
    query = 'query { deal(id: "' + deal_uuid + '" ) { PieceCid InboundFilePath } }'
    payload = {'query': query}
    headers = {'Content-Type': 'application/json'}

    # Send the POST request to the GraphQL endpoint
    response = requests.post('http://localhost:8080/graphql/query', json=payload, headers=headers)

    # Check for HTTP errors
    response.raise_for_status()

    # Parse the JSON response
    out = response.json()
    bpcid = out['data']['deal']['PieceCid']
    if bpcid == piece_cid:
        if out['data']['deal']['InboundFilePath'] == "":
            return True
        else:
            print(f"ERROR: cannot process deal {deal_uuid}: data already imported")
            return False
    else:
        print(f"ERROR: cannot process deal {deal_uuid}: pieceCid mismatch proposal: {piece_cid} and deal: {bpcid}")
        return False


# Provides the current size of download directory
def get_download_dir_size():
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(download_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    return total_size


# Reserves the specified number of deal in spade
def send_deals(c):
    auth_header = generate_spade_auth()

    response = requests.get(eligible_proposals_url, headers=auth_header)
    eligible_proposals = []

    if response.status_code == 200:
        data = response.json()
        print(data)
        print("INFO: Generating a list of eligible proposals")
        for item in data['response']:
            eligible_proposals.append(item)

        if len(eligible_proposals_url) > 0:
            i = 0
            for p in eligible_proposals:
                if i < c:
                    r = p['sample_reserve_cmd']
                    ex = r.split('( ', 2)[1].split(' | ', 3)[0]
                    a = generate_spade_auth(ex)
                    res = response = requests.post(send_deal_url, json={}, headers=a)
                    if response.status_code == 200:
                        i = i + 1
                    else:
                        if eligible_proposals(len(eligible_proposals)-1) == p:
                            print(f"WARN: Only {i} eligible proposals found out of requested {c}")
                else:
                    print(f"INFO: {i} deals sent")
                    break
        else:
            print("WARN: No eligible proposals found")
    else:
        print("ERROR: Eligible proposals request failed with status code:", response.status_code)


def process_proposal(p, t):
    s = query_deal_status(p['deal_proposal_id'], p['piece_cid'])
    if s:
        # TODO: Check space
        # TODO: Check if already being downloaded
        client = aria2p.API(
            aria2p.Client(
                host="http://localhost",
                port=6801,
                secret=t
            )
        )

        gid = client.add_uris(s['data_sources'])
        status = client.get_download(gid)
        failed = False
        while not status.is_complete:
            if status.error_message != "":
                failed = True
                break
            else:
                status = client.get_download(gid)
        if failed:
            print(f"ERROR: Downloads failed for deal id: {p['deal_proposal_id']}: {status.error_message}")
        else:
            print(f"INFO: Downloads complete for deal id: {p['deal_proposal_id']}")
            # TODO: Add boost import-data
    else:
        print(f"ERROR: not processing deal: {p['deal_proposal_id']}")


def thread_monitor(t, i, tpool):
    t.join()
    tpool.removethread(i)


# Start execution
def start():
    try:
        # Start logging
        log_filename = log_directory + "/spade-deal-downloader.log"
        try:
            # Try to open the file for writing
            with open(log_filename, 'x') as f:
                f.write("############################################################\n")
                f.write("################ Starting a new process #####################\n")
                sys.stdout = f
        except FileExistsError:
            # If the file already exists, open it for writing
            with open(log_filename, 'a') as f:
                f.write("############################################################\n")
                f.write("################ Starting a new process #####################\n")
                sys.stdout = f

        pid = os.getpid()
        # Generate a random number for the JWT payload
        payload = {"random_number": random.randint(1, 100)}

        # Define a secret key for the JWT
        secret_key = "mysecretkey"

        # Create the JWT using the payload and secret key
        token = jwt.encode(payload, secret_key, algorithm="HS256")
        aria2c_final_cmd = aria2c_cmd + " --rpc-secret=" + token + " --stop-with-process=" + pid
        try:
            output = subprocess.check_output(aria2c_final_cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: failed to start aira2c daemon {e.returncode}:")
            print(e.output.decode())
        else:
            print(output.decode())

        tpool = ThreadPool()
        processing_proposals = []

        while True:
            if len(processing_proposals) < max_concurrent_proposals:
                sorted_pending_proposals = generate_pending_proposals()
                if len(sorted_pending_proposals) > 0:
                    # Start processing the pending proposals
                    for proposal in sorted_pending_proposals:
                        dealid = proposal['deal_proposal_id']
                        if not tpool.checkthread(dealid):
                            th = threading.Thread(target=process_proposal(proposal, token))
                            pr = {"thread": th, "id": dealid}
                            tpool.threads.append(pr)
                            th.start()
                            processing_proposals.append(dealid)
                            thread_monitor(th, dealid, tpool)
                    time.sleep(60)
                else:
                    send_deals(max_concurrent_proposals - len(processing_proposals))
                    time.sleep(300)
            else:
                time.sleep(60)

    except KeyboardInterrupt:
        # Pause all downloads. The aria2c daemon will stop automatically once script finishes
        client = aria2p.API(
            aria2p.Client(
                host="http://localhost",
                port=6801,
                secret=token
            )
        )
        paused = False
        max_retries = 10
        retry = 1
        while not paused and retry <= max_retries:
            time.sleep(retry)
            paused = client.pause_all()
            if paused:
                break
            else:
                retry = retry + 1

        if paused:
            print("Stopped aria2c daemon gracefully")
        else:
            client.pause_all(True)
            print("Stopped aria2c daemon forcefully")

        print("################ Stopping #####################")


def main():
    start()


if __name__ == "__main__":
    main()
