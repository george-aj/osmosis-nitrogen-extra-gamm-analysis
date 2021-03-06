import base64
import codecs
import csv
import json
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Text, Mapping, Any
from copy import deepcopy

import httplib2

import requests
import cosmpy.protos.osmosis.gamm.pool_models.balancer.balancerPool_pb2
import cosmpy.protos.osmosis.gamm.v1beta1.query_pb2 as query_gamms
from google.protobuf.json_format import MessageToDict

msg_join = '/osmosis.gamm.v1beta1.MsgJoinPool'
msg_exit = '/osmosis.gamm.v1beta1.MsgExitPool'
msg_swap = '/osmosis.gamm.v1beta1.MsgSwapExactAmountIn'
msg_single = '/osmosis.gamm.v1beta1.MsgJoinSwapExternAmountIn'

start_height = 4707301
halt_height = 4713064

file_path = 'C:\\Users\\admin\\Documents\\GitHub\\osmosis-nitrogen-extra-gamm-analysis\\'
# Use a node that isn't too pruned
node_ip = 'NODE_IP'


def parse_log(log, event_type, attribute_type):
    value = None
    for event in log:
        if event.get('type') == event_type:
            for attribute in event.get('attributes'):
                if attribute.get('key') == attribute_type:
                    value = attribute.get('value')

    return value


def parse_coin(coin):
    if 'uosmo' in coin:
        denom = 'uosmo'
        amount = coin.replace('uosmo', '')
    elif 'uion' in coin:
        denom = 'uion'
        amount = coin.replace('uion', '')
    else:
        split = coin.split('ibc/')
        denom = 'ibc/' + split[1]
        amount = split[0]

    return denom, amount


def get_block_and_filter(block_height):
    block_url = 'http://' + node_ip + ':26657/block_results?height=' + str(block_height)

    i = 0
    while True:
        try:
            i += 1

            # Block 4707635 fails to get
            block = requests.get(block_url, timeout=120, stream=True).json()

            break
        except Exception as e:
            sleep(5)

            if i > 10:
                print("Failed to get block: " + str(block_height))
                print(str(e))
                return [[], [], [], []]

    join_rows = []
    exit_rows = []
    swap_rows = []
    single_rows = []

    if block is not None \
            and 'result' in block \
            and 'txs_results' in block.get('result') \
            and block.get('result').get('txs_results') is not None \
            and len(block.get('result').get('txs_results')) > 0:

        for tx in block.get('result').get('txs_results'):
            c = 0
            if tx.get('code') == 0:
                logs = json.loads(tx.get('log'))
                log = logs[c].get('events')

                msg_type = parse_log(log, 'message', 'action')

                row = []
                row.append(block_height)
                row.append(tx.get('code'))
                row.append(msg_type)

                if msg_type == msg_join:
                    row.append(parse_log(log, 'pool_joined', 'sender'))
                    row.append(parse_log(log, 'pool_joined', 'pool_id'))

                    share_out_raw = parse_log(log, 'coinbase', 'amount')
                    share_out = share_out_raw[:share_out_raw.find('gamm/pool/')]

                    row.append(share_out)

                    token_in = parse_log(log, 'pool_joined', 'tokens_in').split(',')

                    for coin in token_in:
                        denom, amount = parse_coin(coin)

                        row.append(denom)
                        row.append(amount)

                    join_rows.append(row)
                elif msg_type == msg_exit:
                    row.append(parse_log(log, 'pool_exited', 'sender'))
                    row.append(parse_log(log, 'pool_exited', 'pool_id'))

                    share_in_raw = parse_log(log, 'burn', 'amount')
                    row.append(share_in_raw[:share_in_raw.find('gamm/pool/')])

                    token_out = parse_log(log, 'pool_exited', 'tokens_out').split(',')

                    for coin in token_out:
                        denom, amount = parse_coin(coin)

                        row.append(denom)
                        row.append(amount)

                    exit_rows.append(row)
                elif msg_type == msg_swap:
                    #split each token swap into its own row
                    for item in log:
                        if item.get('type') == 'token_swapped':
                            j = 0
                            row_temp = deepcopy(row)

                            for attributes in item.get('attributes'):
                                key = attributes.get('key')
                                value = attributes.get('value')

                                if key == 'sender':
                                    row_temp.append(value)
                                    j += 1

                                if key == 'pool_id':
                                    row_temp.append(value)
                                    j += 1

                                if key == 'tokens_in':
                                    denom, amount = parse_coin(value)
                                    row_temp.append(denom)
                                    row_temp.append(amount)
                                    j += 1

                                if key == 'tokens_out':
                                    denom, amount = parse_coin(value)
                                    row_temp.append(denom)
                                    row_temp.append(amount)
                                    j += 1

                                if j == 4:
                                    swap_rows.append(row_temp)
                                    row_temp = deepcopy(row)
                                    j = 0
                elif msg_type == msg_single:
                    row.append(parse_log(log, 'pool_joined', 'sender'))
                    row.append(parse_log(log, 'pool_joined', 'pool_id'))

                    denom_in, amount_in = parse_coin(parse_log(log, 'pool_joined', 'tokens_in'))

                    gamm_out = parse_log(log, 'coinbase', 'amount')

                    denom_out = gamm_out[gamm_out.find('gamm/pool/'):]
                    amount_out = gamm_out[:gamm_out.find('gamm/pool/')]

                    row.append(denom_in)
                    row.append(amount_in)
                    row.append(denom_out)
                    row.append(amount_out)

                    single_rows.append(row)

                c += 1

    return join_rows, exit_rows, swap_rows, single_rows


def write_rows(rows, file_name):
    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def run():
    blocks = []
    join_rows = [['block', 'code', 'msg_type', 'sender', 'pool_id', 'share_out', 'denom_1', 'amount_1', 'denom_2', 'amount_2']]
    exit_rows = [['block', 'code', 'msg_type', 'sender', 'pool_id', 'share_in', 'denom_1', 'amount_1', 'denom_2', 'amount_2', 'denom_3', 'amount_3', 'denom_4', 'amount_4']]
    swap_rows = [['block', 'code', 'msg_type', 'sender', 'pool_id', 'denom_in', 'amount_in', 'denom_out', 'amount_out']]
    single_rows = [['block', 'code', 'msg_type', 'sender', 'pool_id', 'denom_in', 'amount_in', 'denom_out', 'amount_out']]

    for i in range(start_height, halt_height + 1):
        blocks.append(i)

    # If using a public node consider lowering this
    with ThreadPoolExecutor(max_workers=25) as executor:
        for result in executor.map(get_block_and_filter, blocks):
            if len(result[0]) > 0:
                join_rows.extend(result[0])

            if len(result[1]) > 0:
                exit_rows.extend(result[1])

            if len(result[2]) > 0:
                swap_rows += result[2]

            if len(result[3]) > 0:
                single_rows += result[3]

    if join_rows is not None and len(join_rows) > 0:
        write_rows(join_rows, file_path + 'osmosis_joins.csv')

    if exit_rows is not None and len(exit_rows) > 0:
        write_rows(exit_rows, file_path + 'osmosis_exits.csv')

    if swap_rows is not None and len(swap_rows) > 0:
        write_rows(swap_rows, 'osmosis_swaps.csv')

    if single_rows is not None and len(single_rows) > 0:
        write_rows(single_rows, 'osmosis_single_asset.csv')


def calc_share_out_amount(user_token_in, pool_total_shares, pool_token_amount):
    return int(float('{:f}'.format(user_token_in * pool_total_shares / pool_token_amount)))


def get_share_out_min_amount(denom, amount, total_shares, pool_assets, total_weight):
    for asset in pool_assets:
        weight_percent = float(asset.get('weight')) / float(total_weight)
        if asset.get('token').get('denom') == denom:
            return calc_share_out_amount(user_token_in=int(amount * weight_percent),
                                         pool_total_shares=int(total_shares),
                                         pool_token_amount=int(asset.get('token').get('amount')))


def _send_abci_query(request_msg: object, path: Text, response_msg: object, height: int) -> Mapping[Text, Any]:
    """Encode and send pre-filled protobuf msg to RPC endpoint."""
    # Some queries have no data to pass.
    if request_msg:
        request_msg = codecs.encode(request_msg.SerializeToString(), 'hex')
        request_msg = str(request_msg, 'utf-8')

    req = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "abci_query",
        "params": {
            "height": str(height),
            "path": path,
            "data": request_msg
        }
    }
    req = json.dumps(req)
    response = requests.post('http://' + node_ip + ':26657', req).json()
    if 'result' not in response:
        print(response)
    response = response['result']['response']['value']
    response = base64.b64decode(response)
    result = response_msg()
    result.ParseFromString(response)
    result = MessageToDict(result)
    return result


def get_pool_data(height):
    print(str(height))
    request_msg = query_gamms.QueryPoolsRequest()
    request_msg.pagination.limit = 10000
    response_msg = query_gamms.QueryPoolsResponse
    try:
        pool_data = _send_abci_query(request_msg=request_msg,
                                     path="/osmosis.gamm.v1beta1.Query/Pools",
                                     response_msg=response_msg,
                                     height=height)
    except:
        sleep(5)
        pool_data = _send_abci_query(request_msg=request_msg,
                                     path="/osmosis.gamm.v1beta1.Query/Pools",
                                     response_msg=response_msg,
                                     height=height)

    pool_map = {}
    for pool in pool_data.get('pools'):
        pool_map.update({pool.get('id'): pool})

    return [height, pool_map]


# comment out once you have a copy of osmosis_joins.csv, only need to download this once
run()

join_rows = []

with open(file_path + 'osmosis_joins.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        join_rows.append(row)

"""
exit_rows = []

with open(file_path + 'osmosis_exits.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        exit_rows.append(row)
"""

pool_data_map = {}

height_set = set()
for row in join_rows:
    height_set.add(int(row.get('block')) - 1)

with ThreadPoolExecutor(max_workers=20) as executor:
    for result in executor.map(get_pool_data, height_set):
        if len(result) > 0:
            pool_data_map.update({result[0]: result[1]})

rows = []
for row in join_rows:
    height = int(row.get('block'))

    denom_1 = row.get('denom_1')
    denom_2 = row.get('denom_2')

    amount_1 = int(row.get('amount_1'))
    amount_2 = int(row.get('amount_2'))

    pool_data = pool_data_map.get(height - 1).get(row.get('pool_id'))
    total_shares = pool_data.get('totalShares').get('amount')
    pool_assets = pool_data.get('poolAssets')
    total_weight = pool_data.get('totalWeight')

    denom_1_share_out = int(get_share_out_min_amount(denom_1, amount_1, total_shares, pool_assets, total_weight))
    denom_2_share_out = int(get_share_out_min_amount(denom_2, amount_2, total_shares, pool_assets, total_weight))

    share_out_estimate = denom_1_share_out + denom_2_share_out

    recorded_share_out = int(row.get('share_out'))

    ratio = (recorded_share_out / (denom_1_share_out + denom_2_share_out)) - 1

    print(str(denom_1_share_out) + ', ' + str(denom_2_share_out) + ', ' + str(recorded_share_out) + ', ' + str(ratio))
    temp_row = []

    temp_row.append(row.get('sender'))
    temp_row.append(share_out_estimate)
    temp_row.append(recorded_share_out)
    temp_row.append(ratio)

    rows.append(temp_row)

write_rows(rows, file_path + 'osmosis_join_extra_gamm_estimate.csv')

print()
