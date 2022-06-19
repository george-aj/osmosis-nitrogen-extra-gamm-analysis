import json
import csv

def read_file(file_name):
    row_list = []

    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_list.append(row)

    return row_list

#make map by pool_id to over extra issue tokens
def parse_gamm_estimates():
    gamm_estimates_list = read_file('..//osmosis_join_extra_gamm_estimate.csv')
    
    gamm_estimates_map = {}    
    for row in gamm_estimates_list:
        pool_id = row.get('pool_id')
        coin_amount_map = gamm_estimates_map.get(pool_id) or {}

        denom_1 = row.get('denom_1')
        denom_2 = row.get('denom_2')

        amount_1 = int(row.get('amount_1'))
        amount_2 = int(row.get('amount_2'))

        adjustment_ratio = 1 + float(row.get('ratio'))
        
        coins = [{'denom': denom_1, 'amount': amount_1}, {'denom': denom_2, 'amount': amount_2}]        
        
        for coin in coins:
            denom = coin.get('denom')
            extra_amount_total = coin_amount_map.get(denom) or 0
            
            sender_amount = int(coin.get('amount'))            
            amount_extra_issued = sender_amount - (sender_amount / adjustment_ratio)
            extra_amount_total += amount_extra_issued
            
            coin_amount_map.update({denom: extra_amount_total})
            gamm_estimates_map.update({pool_id: coin_amount_map})
            
    return gamm_estimates_map


def calculate_airdrop_amount(sender, sender_gamm_map, airdrop_amount_map, gamm_total_share_map, gamm_estimates_map, validation_map):
    sender_airdrop_map = airdrop_amount_map.get(sender) or {}

    excluded_sender = 'osmo1njty28rqtpw6n59sjj4esw76enp4mg6g7cwrhc'

    if sender == excluded_sender:
        return

    for pool_denom in sender_gamm_map.keys():
        pool_id = pool_denom.replace('gamm/pool/', '')
        sender_gamm_amount = sender_gamm_map.get(pool_denom)
        pool_ownership = sender_gamm_amount / gamm_total_share_map.get(pool_denom)

        # only need to calculate airdrop amount if pool was impacted
        if pool_id in gamm_estimates_map:
            #validate all ownership amounts add up to 100%
            pool_percent_reimbursed = validation_map.get(pool_id) or 0
            pool_percent_reimbursed += pool_ownership
            validation_map.update({pool_id: pool_percent_reimbursed})

            coin_map = gamm_estimates_map.get(pool_id)

            for denom in coin_map:
                amount = coin_map.get(denom)
                previous_airdrop_amount = sender_airdrop_map.get(denom) or 0
                previous_airdrop_amount += pool_ownership * amount

                sender_airdrop_map.update({denom: previous_airdrop_amount})
                airdrop_amount_map.update({sender: sender_airdrop_map})

def run_airdrop_estimate():
    # read in over issued gamm estimates for all joins and reduce by pool_id
    gamm_estimates_map = parse_gamm_estimates()

    #open and load a state export
    f = open('state_export_upgrade_height_4707300.json', 'r+', encoding='utf-8')
    j = json.loads(f.read())
    f.close()

    lock_list = j['app_state']['lockup']['locks']
    balance_list = j['app_state']['bank']['balances']
    gamm_list = j['app_state']['bank']['supply']

    #make a map of all locked gamm by sender and gamm/pool/x
    lock_map = {}
    for lock in lock_list:
        sender = lock.get('owner')
        sender_gamm_map = lock_map.get(sender) or {}
        
        for coin in lock.get('coins'):
            denom = coin.get('denom')
            if 'gamm/pool/' in denom:
                gamm_amount = sender_gamm_map.get(denom) or 0
                gamm_amount += int(coin.get('amount'))
                sender_gamm_map.update({denom: gamm_amount})

                lock_map.update({sender: sender_gamm_map})            

    #make a map of all balances by sender and gamm/pool/x
    balance_map = {}
    for balance in balance_list:
        sender = balance.get('address')
        for coin in balance.get('coins'):
            denom = coin.get('denom')
            sender_gamm_map = balance_map.get(sender) or {}
            
            if 'gamm/pool/' in denom:
                gamm_amount = sender_gamm_map.get(denom) or 0
                gamm_amount += int(coin.get('amount'))
                sender_gamm_map.update({denom: gamm_amount})

                balance_map.update({sender: sender_gamm_map})

    #make a map of all total pool shares by gamm/pool/x
    gamm_total_share_map = {}
    for supply in gamm_list:
        gamm_total_share_map.update({supply.get('denom'): int(supply.get('amount'))})

    #calculate airdrop amount for locked/bonded gamm
    airdrop_amount_map = {}
    validation_map = {}
    for sender in lock_map.keys():
        calculate_airdrop_amount(sender, lock_map.get(sender), airdrop_amount_map, gamm_total_share_map, gamm_estimates_map, validation_map)

        if sender in balance_map:
            print(sender)
            print(lock_map.get(sender))
            print(balance_map.get(sender))

    # calculate airdrop amount for un-locked/un-bonded gamm
    for sender in balance_map.keys():
        calculate_airdrop_amount(sender, balance_map.get(sender), airdrop_amount_map, gamm_total_share_map, gamm_estimates_map, validation_map)

        if sender in lock_map:
            print(sender)
            print(lock_map.get(sender))
            print(balance_map.get(sender))

    #write to file
    with open('osmosis_airdrop_upgrade.csv', "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(['sender', 'denom', 'amount'])

        for sender in airdrop_amount_map.keys():
            sender_coins = airdrop_amount_map.get(sender)
            for denom in sender_coins.keys():
                row = [sender]
                row.append(denom)

                #round up amount to the nearest uToken
                amount = int(sender_coins.get(denom) + 1)

                row.append(amount)
                writer.writerow(row)


run_airdrop_estimate()
