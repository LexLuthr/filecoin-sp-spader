import base64
import json
import os
import sys
import subprocess
import time

import aria2p as aria2p
import requests
from shutil import which

# User must have the latest version of aria2c installed and binary must be found in $PATH variable
# User also needs to export 'LOTUS_FULLNODE_API' and `BOOST_API_INFO` variables


#################### VARIABLES #######################
######################################################

# Number of deals/proposals to be handled simultaneously
max_concurrent_proposals = 10

# Miner ID
spid = "fXXXX"

# Download directory full path (must be owned by current user)
download_dir = "/a/b/c"

# Download directory size in GiBs (limits how many deals are processed at once)
dir_size = 500

# Boost graphql URL
boost_qgl = 'http://localhost:8080/graphql/query'


#################### END OF VARIABLES #######################
#############################################################


# Command used to run the aria2c daemon
aria2c_daemon = "aria2c --daemon --enable-rpc --rpc-listen-port=6801 --keep-unfinished-download-result -s 16 -x 16"
aria2c_session_file = download_dir + "/aria2c.session"
aria2c_session = " --save-session=" + aria2c_session_file + " -i" + aria2c_session_file
aria2c_config = " --auto-file-renaming=false --save-session-interval=2 -j 20 -d" + download_dir + "/download"
aria2c_log = " --log=" + download_dir + "/aria2c.log --log-level=info"
aria2c_cmd = aria2c_daemon + aria2c_session + aria2c_config + aria2c_log

# Spade URLs
pending_proposals_url = "https://api.spade.storage/sp/pending_proposals"
eligible_proposals_url = "https://api.spade.storage/sp/eligible_pieces"
send_deal_url = "https://api.spade.storage/sp/invoke"

# Complete download list
complete_download_list = download_dir + "/completed"

# Failed download list
failed_download_list = download_dir + "/failed"

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


def lotus_apicall(input_data):
    full_node_api = os.environ.get('FULLNODE_API_INFO')
    api_token, api_maddr = full_node_api.strip().split(":")
    ignore, api_nproto, api_host, api_tproto, api_port, api_aproto = api_maddr.split("/")

    if api_nproto == "ip6":
            api_host = f"[{api_host}]"
    cmd = ["/usr/bin/curl", "-m5", "-s", f"http://{api_host}:{api_port}/rpc/v0", "-XPOST", f"-HAuthorization: Bearer {api_token}", "-HContent-Type: application/json", "--data", input_data]
    output = subprocess.run(cmd, capture_output=True, text=True)
    maybe_err = output.stdout
    if not maybe_err:
            raise ValueError(f"Error executing '{input_data}' against API http://{api_host}:{api_port}\n{maybe_err or 'No result from API call'}")
    data = json.loads(output.stdout)
    return data


def gen_auth(extra=None):
    ful_authhdr = "FIL-SPID-V0"

    b64_optional_payload = ""
    if extra:
            b64_optional_payload = base64.b64encode(extra.encode('ascii')).decode('ascii')

    b64_spacepad = "ICAg"
    fil_chain_head = lotus_apicall(f'{{ "jsonrpc": "2.0", "id": 1, "method": "Filecoin.ChainHead", "params": []}}')['result']['Height']
    fil_finalized_tipset = lotus_apicall(f'{{ "jsonrpc": "2.0", "id": 1, "method": "Filecoin.ChainGetTipSetByHeight", "params": [ {fil_chain_head - 900}, null ] }}')['result']['Cids']
    j_fil_finalized_tipset = json.dumps(fil_finalized_tipset)
    fil_finalized_worker_id = lotus_apicall(f'{{ "jsonrpc": "2.0", "id": 1, "method": "Filecoin.StateMinerInfo", "params": [ "{spid}", {j_fil_finalized_tipset} ] }}')['result']['Worker']
    fil_current_drand_b64 = lotus_apicall(f'{{ "jsonrpc": "2.0", "id": 1, "method": "Filecoin.BeaconGetEntry", "params": [ {fil_chain_head} ] }}')['result']['Data']
    fil_authsig = lotus_apicall(f'{{ "jsonrpc": "2.0", "id": 1, "method": "Filecoin.WalletSign", "params": [ "{fil_finalized_worker_id}", "{b64_spacepad}{fil_current_drand_b64}{b64_optional_payload}" ] }}')['result']['Data']


    hdr = f"{ful_authhdr} {fil_chain_head};{spid};{fil_authsig}"
    if extra:
            hdr += f";{b64_optional_payload}"

    return {"Authorization": hdr}


# Generates a list of pending proposals for the miner from Spade API
# List is then sorted based on time remaining to seal the deal
def generate_pending_proposals():
    auth_header = gen_auth()

    response = requests.get(pending_proposals_url, headers=auth_header)
    pending_proposals = []

    if response.status_code == 200:
        data = response.json()
        print("INFO: Generating a list of pending proposals")
        for item in data['response']['pending_proposals']:
            if not find_completed(complete_download_list, item['deal_proposal_id']):
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
    auth_header = gen_auth()

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
                if i < c:
                    r = p['sample_reserve_cmd']
                    ex = r.split("'")[1]
                    a = gen_auth(ex)
                    response = requests.post(send_deal_url, headers=a, allow_redirects=True)
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
    if os.path.exists(file_path):
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
                return False, "", True
            gid = aria_client().client.add_uri(p['data_sources'])
        return True, gid, False
    else:
        return False, "", False


# Monitors the deal threads and cleans up the finished threads
def download_monitor(g, pid):
    status = aria_client().client.tell_status(g)
    print(status)
    if status['status'] == 'complete' and status['errorMessage'] == '':
        print(f"INFO: Downloads complete for deal id: {pid}")
        return False, False, status['files'][0]['path']
    elif status['status'] == 'error' and (status['errorCode'] == '0' or status['errorCode'] == '13'):
        print(f"INFO: Downloads complete for deal id: {pid}")
        return False, False, status['files'][0]['path']
    elif status['status'] == 'removed':
        print(f"INFO: Downloads removed for deal id: {pid}")
        return False, False, status['files'][0]['path']
    elif status['status'] == 'error' and not (status['errorCode'] == '0' or status['errorCode'] == '13'):
        print(f"ERROR: Downloads failed for deal id: {pid}: {status['errorMessage']}")
        return False, True, status['files'][0]['path']
    elif status['status'] == 'active' or status['status'] == 'paused' or status['status'] == 'waiting':
        return True, False, ""
    else:
        return True, False, ""


def boost_api_call(params):
    full_node_api = os.environ.get('BOOST_API_INFO')
    bapi_token, bapi_maddr = full_node_api.strip().split(":")
    ignore, bapi_nproto, bapi_host, bapi_tproto, bapi_port, bapi_aproto = bapi_maddr.split("/")
    if bapi_nproto == "ip6":
        bapi_host = f"[{bapi_host}]"

    bheaders = {"Authorization": f"Bearer {bapi_token}", "content-type": "application/json"}
    burl = f"http://{bapi_host}:{bapi_port}/rpc/v0"

    res = requests.post(burl, data=json.dumps(params), headers=bheaders)
    if res.status_code == 200:
        print(res.json()['result'])
    else:
        print(f"Error executing '{params}' against API http://{bapi_host}:{bapi_port}")
    return


# Call Boost API to start deal execution
def boost_execute():
    if not os.path.exists(complete_download_list):
        return

    with open(complete_download_list, 'r') as file:
        lines = file.readlines()
        for line in lines:
            l = line.split()
            i = l[0]
            f = l[1]
            query = 'query { deal(id: "' + i + '" ) { InboundFilePath } }'
            payload = {'query': query}
            headers = {'Content-Type': 'application/json'}
            response = requests.post(boost_qgl, json=payload, headers=headers)
            response.raise_for_status()
            out = response.json()
            if out['data']['deal']['InboundFilePath'] == "":
                payload = {
                    "method": "Filecoin.BoostOfflineDealWithData",
                    "params": [
                        i,
                        f,
                        True,
                    ],
                    "jsonrpc": "2.0",
                    "id": 1,
                }
                boost_api_call(payload)
    return


# Starts execution loop
def start():
    try:
        # Start logging
        log_filename = download_dir + "/spader.log"
        f = open(log_filename, 'a')
        f.write("############################################################\n")
        f.write("################ Starting a new process #####################\n")
        sys.stdout = f

        sf = open(aria2c_session_file, 'w')
        sf.close()

        pid = os.getpid()
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
            boost_execute()
            f.flush()

            if len(pool) > 0:
                for key in pool.keys():
                    s, fail, path = download_monitor(pool[key], key)
                    f.flush()
                    if not s:
                        completed[key] = pool[key]
                        if fail:
                            complete = open(failed_download_list, 'a')
                            complete.write(f"{key} {path}\n")
                            complete.flush()
                            complete.close()
                        if not fail and not find_completed(complete_download_list, key):
                            complete = open(complete_download_list, 'a')
                            complete.write(f"{key} {path}\n")
                            complete.flush()
                            complete.close()

                if len(completed) > 0:
                    for key in completed:
                        pool.pop(key, None)

            if len(pool) < max_concurrent_proposals:
                process_next_time = 0
                sorted_pending_proposals = generate_pending_proposals()
                # Start processing the pending proposals
                if len(sorted_pending_proposals) > 0:
                    for proposal in sorted_pending_proposals:
                        dealid = proposal['deal_proposal_id']
                        if dealid not in pool and dealid not in completed:
                            m, did, lack_of_space = process_proposal(proposal)
                            f.flush()
                            if m:
                                pool[dealid] = did
                            elif lack_of_space:
                                process_next_time += 1

                send_deals(max_concurrent_proposals - len(pool) - process_next_time)
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
