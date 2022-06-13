import base64
import codecs
import csv
import json
from concurrent.futures import ThreadPoolExecutor
from copy import copy, deepcopy
from statistics import median, mean
from time import sleep
from typing import Text, Mapping, Any

import cosmpy.protos.osmosis.gamm.pool_models.balancer.balancerPool_pb2
import cosmpy.protos.osmosis.gamm.v1beta1.query_pb2 as query_gamms
import requests
from google.protobuf.json_format import MessageToDict

file_path = 'C:\\Users\\admin\\Google Drive\\Osmosis v9\\'
node_ip = 'NODE_IP'

start_height = 4707301
halt_height = 4713064

msg_join = '/osmosis.gamm.v1beta1.MsgJoinPool'
msg_exit = '/osmosis.gamm.v1beta1.MsgExitPool'

#If running for the first time set to true, thenk once you have all the CSV files you can set it to false so it doesn't keep fetching the data
first_load = False

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

def write_list_of_dicts(rows, file_name):
    keys = rows[0].keys()

    with open(file_name, 'w', newline="") as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(rows)

def read_file(file_name):
    row_list = []

    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_list.append(row)

    return row_list

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

#Not accurate
def estimate_no_match_amounts_againts_upgrade_height(no_match_list, pool_map):
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

def get_pool_data(height):
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
        try:
            pool_data = _send_abci_query(request_msg=request_msg,
                                         path="/osmosis.gamm.v1beta1.Query/Pools",
                                         response_msg=response_msg,
                                         height=height)
        except Exception as e:
            print(str(height))
            print(e)

    pool_map = {}
    for pool in pool_data.get('pools'):
        pool_map.update({pool.get('id'): pool})

    return [height, pool_map]

def calc_share_out_amount(user_token_in, pool_total_shares, pool_token_amount):
    return int(float('{:f}'.format(user_token_in * pool_total_shares / pool_token_amount)))

def get_share_out_min_amount(denom, amount, total_shares, pool_assets, total_weight):
    for asset in pool_assets:
        weight_percent = float(asset.get('weight')) / float(total_weight)
        if asset.get('token').get('denom') == denom:
            return calc_share_out_amount(user_token_in=int(amount) * weight_percent,
                                         pool_total_shares=int(total_shares),
                                         pool_token_amount=int(asset.get('token').get('amount')))

def calc_share_out_on_join(row, pool_data):
    height = int(row.get('block'))

    denom_1 = row.get('denom_1')
    denom_2 = row.get('denom_2')

    amount_1 = int(row.get('amount_1'))
    amount_2 = int(row.get('amount_2'))

    total_shares = pool_data.get('totalShares').get('amount')
    pool_assets = pool_data.get('poolAssets')
    total_weight = pool_data.get('totalWeight')

    denom_1_share_out = int(get_share_out_min_amount(denom_1, amount_1, total_shares, pool_assets, total_weight))
    denom_2_share_out = int(get_share_out_min_amount(denom_2, amount_2, total_shares, pool_assets, total_weight))

    share_out_estimate = denom_1_share_out + denom_2_share_out

    return share_out_estimate

join_rows = read_file(file_path + 'osmosis_joins.csv')

join_pool_set = set()
for row in join_rows:
    join_pool_set.add(row.get('pool_id'))

exit_rows = read_file(file_path + 'osmosis_exits.csv')

exit_pool_set = set()
for row in exit_rows:
    exit_pool_set.add(row.get('pool_id'))

no_impact = exit_pool_set - join_pool_set

join_map = make_keyed_map(join_rows)
exit_map = make_keyed_map(exit_rows)

if first_load:
    headers = ['block', 'code', 'msg_type', 'sender', 'pool_id', 'share_in', 'denom_1', 'amount_1', 'denom_2', 'amount_2', 'denom_3', 'amount_3', 'denom_4', 'amount_4']
    impacted_exits = [headers]
    not_impacted_exits = [headers]
    match_list = [headers]
    no_match_list = [headers]

    for exit_key in exit_map.keys():
        exit_rows_list = exit_map.get(exit_key)

        for row in exit_rows_list:
            if exit_key in join_map:
                match_list.append(row.values())
            else:
                no_match_list.append(row.values())

            if row.get('pool_id') in no_impact:
                not_impacted_exits.append(row.values())
            else:
                impacted_exits.append(row.values())

    write_rows(match_list, file_path + "osmosis_exit_match_join.csv")
    write_rows(no_match_list, file_path + "osmosis_exit_do_not_match_join.csv")
    write_rows(not_impacted_exits, file_path + "osmosis_not_impacted_exits.csv")
    write_rows(impacted_exits, file_path + "osmosis_impacted_exits.csv")
else:
    match_list = read_file(file_path + "osmosis_exit_match_join.csv")
    no_match_list = read_file(file_path + "osmosis_exit_do_not_match_join.csv")
    not_impacted_exits = read_file(file_path + "osmosis_not_impacted_exits.csv")
    impacted_exits = read_file(file_path + "osmosis_impacted_exits.csv")

join_rows_map_by_sender = {}

for row in join_rows:
    sender = row.get('sender')

    sender_row_list = join_rows_map_by_sender.get(sender) or []
    sender_row_list.append(row)
    join_rows_map_by_sender.update({sender: sender_row_list})

#exits that suffered no loss and do not have a corresponding join
impacted_but_clean_exits = []
not_impacted_pool_has_no_joins = []
exits_with_joins = []

impacted_exits = read_file(file_path + "osmosis_impacted_exits.csv")

if first_load:
    for exit_row in exit_rows:
        exit_block = exit_row.get('block')
        exit_sender = exit_row.get('sender')
        exit_pool_id = exit_row.get('pool_id')

        if exit_pool_id in no_impact:
            not_impacted_pool_has_no_joins.append(exit_row)
            continue

        include = True
        for join_row in join_rows:
            join_block = join_row.get('block')
            join_sender = join_row.get('sender')
            join_pool_id = join_row.get('pool_id')

            if exit_sender == join_sender and exit_pool_id == join_pool_id and exit_block > join_block:
                include = False

        if include:
            impacted_but_clean_exits.append(exit_row)
        else:
            exits_with_joins.append(exit_row)

    write_list_of_dicts(impacted_but_clean_exits, file_path + 'impacted_but_clean_exits.csv')
    write_list_of_dicts(not_impacted_pool_has_no_joins, file_path + 'not_impacted_pool_has_no_joins.csv')
    write_list_of_dicts(exits_with_joins, file_path + 'exits_with_joins.csv')
else:
    impacted_but_clean_exits = read_file(file_path + 'impacted_but_clean_exits.csv')
    not_impacted_pool_has_no_joins = read_file(file_path + 'not_impacted_pool_has_no_joins.csv')
    exits_with_joins = read_file(file_path + 'exits_with_joins.csv')

pool_data_map = {}

height_set = set()
for row in join_rows:
    height_set.add(int(row.get('block')) - 1)

for row in exit_rows:
    height_set.add(int(row.get('block')) - 1)

if first_load:
    with ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(get_pool_data, height_set):
            file = file_path + 'pool_data\\pool_data_at_' + str(result[0]) + '.json'
            if len(result) > 0:
                with open(file, 'w') as f:
                    json.dump(result[1], f)

                pool_data_map.update({result[0]: result[1]})
else:
    for height in height_set:
        f = open(file_path + 'pool_data\\pool_data_at_' + str(height) + '.json', 'r+', encoding='utf-8')
        pool_data_map.update({str(height): json.loads(f.read())})
        f.close()

#interlace the join and exit rows
per_block_msg_map = {}
for row in join_rows:
    height = row.get('block')
    msg_list = per_block_msg_map.get(height) or []
    msg_list.append(row)
    per_block_msg_map.update({height: msg_list})

for row in exit_rows:
    height = row.get('block')
    msg_list = per_block_msg_map.get(height) or []
    msg_list.append(row)
    per_block_msg_map.update({height: msg_list})

estimated_join_gamms = read_file(file_path + 'osmosis_join_extra_gamm_estimate.csv')

pool_ratio_avg_map = {}
extra_amount_ratio_map = {}
for row in estimated_join_gamms:
    pool_id = row.get('pool_id')
    ratio = float(row.get('ratio'))

    if ratio > .0000000001:
        ratio_list = extra_amount_ratio_map.get(pool_id) or []
        ratio_list.append(float(row.get('ratio')))
        extra_amount_ratio_map.update({pool_id: ratio_list})

        pool_ratio_avg_map.update({pool_id: mean(ratio_list)})

#build a second set of pool data to allow for estimating actual amounts people should have gotten when they exited pools
shadow_pool_data_map = {}

#keep a log of changes
shadow_pool_changes_list = [['block', 'sender', 'pool_id', 'msg_type', 'original_shares', 'share_adjustment', 'new_total_shares', 'was_exit_clean', 'sender_original_amount_1', 'sender_original_amount_2', 'pool_denom_1', 'pool_original_amount_1', 'pool_adjustment_amount_1', 'pool_denom_2', 'pool_original_amount_2', 'pool_adjustment_amount_2', 'adjustment_ratio']]

for block in sorted(per_block_msg_map.keys()):
    #get pool data as of the previous block
    actual_pool_data = pool_data_map.get(str(int(block) - 1))

    #get all msgs that happened in the current block
    msg_list = per_block_msg_map.get(block)

    #for each msg adjust pool data
    for row in msg_list:
        exit_block = row.get('block')
        exit_sender = row.get('sender')
        exit_pool_id = row.get('pool_id')
        msg_type = row.get('msg_type')

        #log
        shadow_pool_change = [exit_block, exit_sender, exit_pool_id]

        #get actual pool data in case a shadow pool doesn't exist yet
        actual_pool = actual_pool_data.get(exit_pool_id)


        if exit_pool_id in shadow_pool_data_map:
            shadow_pool = shadow_pool_data_map.get(exit_pool_id)
        else:
            shadow_pool = deepcopy(actual_pool)

        shadow_total_shares_map = shadow_pool.get('totalShares')
        shadow_total_shares_amount = int(shadow_total_shares_map.get('amount'))

        # log
        shadow_pool_change.append(msg_type)
        shadow_pool_change.append(deepcopy(shadow_total_shares_amount))

        if msg_type == msg_join:
            recorded_share_out = int(row.get('share_out'))
            estimate_share_out = calc_share_out_on_join(row, shadow_pool)

            #amount to decrease total gamm shares
            gamm_adjustment = recorded_share_out + estimate_share_out

            # adjust shadow pool by decreasing the gamm share amount that was issued by the extra amount on join
            shadow_total_shares_amount -= gamm_adjustment

            #log
            shadow_pool_change.append(gamm_adjustment)
            shadow_pool_change.append(deepcopy(shadow_total_shares_amount))
        else:
            recorded_share_in = int(row.get('share_in'))

            #if exit does not have a corresponding join or it's unimpacted pool remove gamm shares and pool assets with no adjustment
            if row in impacted_but_clean_exits or row in not_impacted_pool_has_no_joins:
                sender_share_of_pool = recorded_share_in / shadow_total_shares_amount

                shadow_total_shares_amount -= recorded_share_in

                denom_1 = row.get('denom_1')
                denom_2 = row.get('denom_2')

                amount_1 = int(row.get('amount_1'))
                amount_2 = int(row.get('amount_2'))

                # log
                shadow_pool_change.append(recorded_share_in)
                shadow_pool_change.append(deepcopy(shadow_total_shares_amount))
                shadow_pool_change.append(1)
                shadow_pool_change.append(amount_1)
                shadow_pool_change.append(amount_2)

                token_list = shadow_pool.get('poolAssets')

                for token in token_list:
                    token_map = token.get('token')
                    shadow_pool_token_amount = int(token_map.get('amount'))
                    denom = token_map.get('denom')

                    #log
                    shadow_pool_change.append(denom)
                    shadow_pool_change.append(deepcopy(shadow_pool_token_amount))

                    if denom == denom_1:
                        amount_1 = int(sender_share_of_pool * shadow_pool_token_amount)
                        shadow_pool_token_amount -= amount_1
                        token_map.update({'amount': str(shadow_pool_token_amount)})

                        #log
                        shadow_pool_change.append(amount_1)

                    if denom == denom_2:
                        amount_2 = int(sender_share_of_pool * shadow_pool_token_amount)
                        shadow_pool_token_amount -= amount_2
                        token_map.update({'amount': str(shadow_pool_token_amount)})

                        #log
                        shadow_pool_change.append(amount_2)

                    token.update({'token': token_map})

                shadow_pool.update({'poolAssets': token_list})

            #if exit does have a corresponding join remove their gamm shares with no adjustment
            #and pool assets with an adjustment of the extra amount they got, add back ill-gotten assets back into the pool
            else:
                pool_extra_ratio = 1 + pool_ratio_avg_map.get(row.get('pool_id'))

                adjustment_gamm = int(float('{:f}'.format(recorded_share_in / pool_extra_ratio)))
                #Remove an adjusted amount of gamm shares, the ill-gotten amount was already removed on their join.
                shadow_total_shares_amount -= adjustment_gamm

                denom_1 = row.get('denom_1')
                denom_2 = row.get('denom_2')

                amount_1 = int(row.get('amount_1'))
                amount_2 = int(row.get('amount_2'))

                # log
                shadow_pool_change.append(deepcopy(adjustment_gamm))
                shadow_pool_change.append(deepcopy(shadow_total_shares_amount))
                shadow_pool_change.append(0)
                shadow_pool_change.append(amount_1)
                shadow_pool_change.append(amount_2)

                token_list = shadow_pool.get('poolAssets')

                for token in token_list:
                    token_map = token.get('token')
                    shadow_pool_token_amount = int(token_map.get('amount'))
                    denom = token_map.get('denom')

                    # log
                    shadow_pool_change.append(denom)
                    shadow_pool_change.append(deepcopy(shadow_pool_token_amount))

                    if denom == denom_1:
                        adjustment_amount = amount_1 - int(amount_1 / pool_extra_ratio)
                        shadow_pool_token_amount += adjustment_amount
                        token_map.update({'amount': str(shadow_pool_token_amount)})

                        # log
                        shadow_pool_change.append(adjustment_amount)

                    if denom == denom_2:
                        adjustment_amount = amount_2 - int(amount_2 / pool_extra_ratio)
                        shadow_pool_token_amount += adjustment_amount
                        token_map.update({'amount': str(shadow_pool_token_amount)})

                        # log
                        shadow_pool_change.append(adjustment_amount)

                    token.update({'token': token_map})

                shadow_pool.update({'poolAssets': token_list})

                #log
                shadow_pool_change.append(pool_extra_ratio)

        shadow_total_shares_map.update({'amount': str(shadow_total_shares_amount)})
        shadow_pool.update({'totalShares': shadow_total_shares_map})

        shadow_pool_data_map.update({exit_pool_id: shadow_pool})

        shadow_pool_changes_list.append(shadow_pool_change)

for shadow_pool_id in shadow_pool_data_map.keys():
    with open(file_path + 'shadow_pool_data\\pools\\' + shadow_pool_id + '.json', 'w') as f:
        json.dump(shadow_pool_data_map.get(shadow_pool_id), f)

write_rows(shadow_pool_changes_list, file_path + 'shadow_pool_data\\shadow_pool_changes.csv')
