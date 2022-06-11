import base64
import codecs
import csv
import json
from typing import Text, Mapping, Any

import cosmpy.protos.osmosis.gamm.pool_models.balancer.balancerPool_pb2
import cosmpy.protos.osmosis.gamm.v1beta1.query_pb2 as query_gamms
import requests
from google.protobuf.json_format import MessageToDict

file_path = 'C:\\'
node_ip = 'NODE_IP'

start_height = 4707301
halt_height = 4713064


def make_keyed_map(row_list):
    keyed_map = {}

    for row in row_list:
        key = row.get('sender') + row.get('pool_id')
        exit_list = keyed_map.get(key) or []
        exit_list.append(row)
        keyed_map.update({key: exit_list})

    return keyed_map


def write_rows(rows, file_name):
    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


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


def calc_token_out_amounts(pool_data, share_in):
    total_share = int(pool_data.get('totalShares').get('amount'))

    user_ratio = float(share_in) / total_share

    token_list = []

    for asset in pool_data.get('poolAssets'):
        token = asset.get('token')
        token_list.append(token.get('denom'))
        amount_raw = int(token.get('amount')) * user_ratio
        token_list.append(int(amount_raw))

    return token_list


join_rows = []

with open(file_path + 'osmosis_joins.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        join_rows.append(row)

exit_rows = []

with open(file_path + 'osmosis_exits.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        exit_rows.append(row)

join_map = make_keyed_map(join_rows)
exit_map = make_keyed_map(exit_rows)

match_list = [
    ['block', 'code', 'msg_type', 'sender', 'pool_id', 'share_in', 'denom_1', 'amount_1', 'denom_2', 'amount_2',
     'denom_3', 'amount_3', 'denom_4', 'amount_4']]
no_match_list = [
    ['block', 'code', 'msg_type', 'sender', 'pool_id', 'share_in', 'denom_1', 'amount_1', 'denom_2', 'amount_2',
     'denom_3', 'amount_3', 'denom_4', 'amount_4']]
for exit_key in exit_map.keys():
    exit_rows = exit_map.get(exit_key)

    for row in exit_rows:
        if exit_key in join_map:
            match_list.append(row.values())
        else:
            no_match_list.append(row.values())

write_rows(match_list, file_path + "osmosis_exits_match_join.csv")
write_rows(no_match_list, file_path + "osmosis_exits_do_not_match_join.csv")

request_msg = query_gamms.QueryPoolsRequest()
request_msg.pagination.limit = 10000
response_msg = query_gamms.QueryPoolsResponse

pool_data = _send_abci_query(request_msg=request_msg,
                             path="/osmosis.gamm.v1beta1.Query/Pools",
                             response_msg=response_msg,
                             height=start_height)

pool_map = {}
for pool in pool_data.get('pools'):
    pool_map.update({pool.get('id'): pool})

no_match_list = []
with open(file_path + 'osmosis_exits_do_not_match_join.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        no_match_list.append(row)

no_match_amounts = [
    ['sender', 'denom_1', 'amount_1', 'denom_2', 'amount_2', 'denom_3', 'amount_3', 'denom_4', 'amount_4']]
c = 0
for row in no_match_list:
    new_row = []
    pool_data = pool_map.get(row.get('pool_id'))

    new_row.append(row.get('sender'))
    tokens = calc_token_out_amounts(pool_data, row.get('share_in'))

    for token in tokens:
        new_row.append(token)

    no_match_amounts.append(new_row)

write_rows(no_match_amounts, file_path + "osmosis_no_match_amount_at_upgrade_height.csv")
