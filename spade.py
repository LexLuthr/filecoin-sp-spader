import os
import random
import sys
import subprocess
import time
import jwt

import aria2p as aria2p
import requests
from shutil import which

# User must have the latest version of aria2c installed and binary must be found in $PATH variable
# User must have the 'fil-spid.bash' auth script available and executable. It can be downloaded with below URL
# "curl -OL https://raw.githubusercontent.com/ribasushi/bash-fil-spid-v0/5f41eec1a/fil-spid.bash"
# User also needs to export 'LOTUS_FULLNODE_API' variable to allow the above script to work

# Number of deals/proposals to be handled simultaneously
max_concurrent_proposals = 10

# Miner ID
spid = "fXXXX"

# Download directory full path (must be owned by current user)
download_dir = "/a/b/c"

# Download directory size in GiBs (limits how many deals are processed at once)
dir_size = 500

# Spade authenticator script (fil-spid.bash) location (must be full path)
spade_script = "/a/b/c"

# Boost graphql URL
boost_qgl = 'http://localhost:8080/graphql/query'

# Command used to run the aria2c daemon
aria2c_daemon = "aria2c --daemon --enable-rpc --rpc-listen-port=6801 --keep-unfinished-download-result"
aria2c_session_file = download_dir + "/aria2c.session"
aria2c_session = " --save-session=" + aria2c_session_file + " -i" + aria2c_session_file
aria2c_config = " --auto-file-renaming=false --save-session-interval=2 -j 20 -d" + download_dir + "/download"
aria2c_log = " --log=" + download_dir + "/aria2c.log"
aria2c_cmd = aria2c_daemon + aria2c_session + aria2c_config + aria2c_log

# Spade URLs
pending_proposals_url = "https://api.spade.storage/sp/pending_proposals"
eligible_proposals_url = "https://api.spade.storage/sp/eligible_pieces"
send_deal_url = "https://api.spade.storage/sp/invoke"

# Complete download list file
complete_download_list_file = download_dir + "/completed"


# Creates an aria2p client
def aria_client():
    return aria2p.API(
        aria2p.Client(
            host="http://localhost",
            port=6801,
            secret=""
        )
    )


# Provides the current size of download directory
def get_download_dir_size():
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(download_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    return total_size


def setup():
    # Check if aria2c exists
    if which("aria2c") is None:
        print(f"Error: Utility aria2c does not exist")
        sys.exit(1)

    # Check if log directory exists
    if not os.path.exists(download_dir):
        print(f"Error: Download directory {download_dir} does not exist")
        sys.exit(1)

    # Check if download directory exists
    if not os.path.exists(download_dir):
        print(f"Error: Download directory {download_dir} does not exist")
        sys.exit(1)

    # Check if download directory has enough free space
    fs_size = os.statvfs(download_dir).f_frsize * os.statvfs(download_dir).f_bavail
    if fs_size < ((dir_size * 1024 * 1024 * 1024) - get_download_dir_size()):
        print(
            f"Error: Download directory file system does not have enough space to accommodate full download directory")
        sys.exit(1)

    # Check if spade script exists
    if not os.path.exists(spade_script):
        print(f"Error: Spade script {spade_script} does not exist")
        sys.exit(1)


def generate_spade_auth(extra=None):
    if extra:
        auth_token_p = subprocess.Popen(['echo', ' -n', extra], stdout=subprocess.PIPE)
        auth_token = subprocess.check_output([spade_script, spid], stdin=auth_token_p.stdout).decode().strip()
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
        print("INFO: Generating a list of pending proposals")
        for item in data['response']['pending_proposals']:
            if not find_completed(complete_download_list_file, item['deal_proposal_id']):
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
    response = requests.post(boost_qgl, json=payload, headers=headers)

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


# Reserves the specified number of deal in spade
def send_deals(c):
    if c <= 0:
        return
    auth_header = generate_spade_auth()

    response = requests.get(eligible_proposals_url, headers=auth_header)
    eligible_proposals = []

    if response.status_code == 200:
        data = response.json()
        print("INFO: Generating a list of eligible proposals")
        for item in data['response']:
            eligible_proposals.append(item)

        if len(eligible_proposals_url) > 0:
            i = 0
            for p in eligible_proposals:
                if i >= c:
                    break
                r = p['sample_reserve_cmd']
                ex = r.split("'")[1]
                a = generate_spade_auth(ex)
                response = requests.post(send_deal_url, json={}, headers=a)
                if response.status_code == 200:
                    i = i + 1

            if i < c:
                print(f"WARN: Only {i} eligible proposals found out of requested {c}")
            else:
                print(f"INFO: {i} deals sent")

            time.sleep(300)
        else:
            print("WARN: No eligible proposals found")
    else:
        print("ERROR: Eligible proposals request failed with status code:", response.status_code)


def find_gid(file_path, uri):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for i, line in enumerate(lines):
            if uri in line:
                for next_line in lines[i + 1:]:
                    if 'gid' in next_line:
                        return next_line.strip()
                break
    return None


def find_completed(file_path, i):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if i in line:
                return True
    return False


# Processes a deal proposal from spade:
# 1. Check if download already in progress
# 2. Check if we have enough space in download directory
# 3. Queue the download and wait for it finish or error out
# 4. Call Boost API to import the data for deal TODO
def process_proposal(p):
    print(f"INFO: Processing deal {p['deal_proposal_id']}")
    piece_size = p['piece_size']
    s = query_deal_status(p['deal_proposal_id'], p['piece_cid'])
    if s:
        gid = ""
        for i in p['data_sources']:
            gid_str = find_gid(aria2c_session_file, i)
            if gid_str is not None:
                gid = gid_str.split('=')[1]
                break

        if gid == "":
            current_size = get_download_dir_size()
            if (current_size + piece_size) > dir_size * 1024 * 1024 * 1024:
                print(f"INFO: Not enough space for deal id: {p['deal_proposal_id']}")
                return False, gid
            gid = aria_client().client.add_uri(p['data_sources'])
        return True, gid
    else:
        return False, ""


# Monitors the deal threads and cleans up the finished threads
def download_monitor(g, pid):
    status = aria_client().client.tell_status(g)
    print(status)
    if status['status'] != 'complete' and 'errorMessage' not in status:
        return True
    if status['status'] != 'complete' and 'errorCode' in status:
        if status['errorCode'] == str(13):
            print(f"INFO: Downloads complete for deal id: {pid}")
        else:
            print(f"ERROR: Downloads failed for deal id: {pid}: {status['errorMessage']}")
        return False
    if status['status'] == 'complete' and 'errorMessage' in status:
        print(f"ERROR: Downloads failed for deal id: {pid}: {status['errorMessage']}")
        return False


# Starts execution loop
def start():
    try:
        # Start logging
        log_filename = download_dir + "/spade-deal-downloader.log"
        f = open(log_filename, 'a')
        f.write("############################################################\n")
        f.write("################ Starting a new process #####################\n")
        sys.stdout = f

        sf = open(aria2c_session_file, 'w')
        sf.close()

        pid = os.getpid()
        # Generate a random number for the JWT payload
        payload = {"random_number": random.randint(1, 100)}

        # Define a secret key for the JWT
        secret_key = "mysecretkey"

        # Create the JWT using the payload and secret key
        token = jwt.encode(payload, secret_key, algorithm="HS256")
        aria2c_final_cmd = aria2c_cmd + " --stop-with-process=" + str(pid)
        try:
            output = subprocess.check_output(aria2c_final_cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: failed to start aira2c daemon {e.returncode}:")
            print(e.output.decode())
        else:
            print(output.decode())

        if not os.path.exists(download_dir + "/download"):
            os.makedirs(download_dir + "/download")
            print(f"Directory '{download_dir}'/download created.")
        else:
            print(f"Directory '{download_dir}'/download already exists.")

        pool = {}
        completed = {}

        while True:
            if len(pool) > 0:
                for key in pool.keys():
                    s = download_monitor(pool[key], key)
                    f.flush()
                    if not s:
                        completed[key] = pool.get(key)

                if len(completed) > 0:
                    for key in completed:
                        if not find_completed(complete_download_list_file, key):
                            complete = open(complete_download_list_file, 'a')
                            complete.write(f"{key}\n")
                            complete.flush()
                            pool.pop(key)

            if len(pool) < max_concurrent_proposals:
                sorted_pending_proposals = generate_pending_proposals()
                # Start processing the pending proposals
                if len(sorted_pending_proposals) > 0:
                    for proposal in sorted_pending_proposals:
                        dealid = proposal['deal_proposal_id']
                        if dealid not in pool and dealid not in completed:
                            m, did = process_proposal(proposal)
                            f.flush()
                            if m:
                                pool[dealid] = did

                send_deals(max_concurrent_proposals - len(pool))
                f.flush()

            else:
                time.sleep(30)

    except KeyboardInterrupt:
        # Pause all downloads. The aria2c daemon will stop automatically once script finishes
        paused = False
        max_retries = 10
        retry = 1
        while not paused and retry <= max_retries:
            time.sleep(retry)
            paused = aria_client().pause_all()
            if paused:
                break
            else:
                retry = retry + 1

        if paused:
            time.sleep(3)  # To allow session to be saved
            print("Stopped aria2c daemon gracefully")
        else:
            aria_client().pause_all(True)
            time.sleep(3)  # To allow session to be saved
            print("Stopped aria2c daemon forcefully")

        print("################ Stopping #####################")
        f.close()


def main():
    start()


if __name__ == "__main__":
    main()
