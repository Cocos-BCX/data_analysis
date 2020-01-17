# -*- coding:utf-8 -*-

import pymongo
from config import *
from PythonMiddlewarebase.operationids import operations
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway
from utils import Logging

logger = Logging().getLogger()

def init_gauges():
    registry = CollectorRegistry()
    gauges['registry'] = registry

    g_account_create = Gauge('account_create', '创建账号', ['env', 'block_num', 'transaction_id', 'account_name', 'account_id', 'time', 'op'], registry=registry)
    gauges[operations['account_create']] = g_account_create

    g_contract_create = Gauge('contract_create', '创建合约', ['env', 'block_num', 'transaction_id', 'contract_name', 'contract_id', 'time', 'creator', 'op'], registry=registry)
    gauges[operations['contract_create']] = g_contract_create

    g_asset_create = Gauge('asset_create', '创建同质资产', ['env', 'block_num', 'transaction_id', 'issuer', 'symbol', 'precision', 'max_supply', 'asset_id', 'time', 'op'], registry=registry)
    gauges[operations['asset_create']] = g_asset_create

    g_nh_asset_create = Gauge('nh_asset_create', '创建非同质资产', ['env', 'block_num', 'transaction_id', 'owner', 'asset_id', 'world_view', 'nh_asset_id', 'time', 'op'], registry=registry)
    gauges[operations['create_nh_asset']] = g_nh_asset_create

    g_nh_asset_order_fill = Gauge('nh_asset_order_filled', '非同质资产订单成交', ['env', 'block_num', 'transaction_id', 'order','nh_asset', 'price', 'price_asset', 'price_asset_symbol', 'time', 'op'], registry=registry)
    gauges[operations['fill_nh_asset_order']] = g_nh_asset_order_fill

    g_block_transaction = Gauge('transaction', '区块交易数据', ['env', 'block_num', 'transaction_id', 'time', 'operation_ids', 'operations_total'], registry=registry)
    gauges[9999] = g_block_transaction
    logger.info('gauges init done. keys: {}'.format(gauges.keys()))

def parse_operations(gph, block_num, block_time, transactions):
    operations_list = []
    for tx in transactions:
        transaction_id = tx[0]
        op_count = 0
        op_ids = []
        for op in tx[1]['operations']:
            op_id = op[0]
            op_ids.append(op_id)
            op_data = {"block_num": block_num, "time": block_time, "transaction_id": transaction_id, "op_id": op_id}
            if op_id == operations['transfer']:
                from_account = op[1]['from']
                to_account = op[1]['to']
                try:
                    from_account = gph.rpc.get_object(op[1]['from'])['name']
                    to_account = gph.rpc.get_object(op[1]['to'])['name']
                except Exception as e:
                    logger.error("block: {}, trx: {}, op: {}, get_object error: {}---'{}'".format(block_num, transaction_id, op_id, repr(e)))
                op_data["from"] = from_account
                op_data["to"] = to_account
                op_data["amount_asset_id"] = op[1]['amount']['asset_id']
                op_data["amount_amount"] = op[1]['amount']['amount']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['account_create']:
                op_data["account_name"] = op[1]['name']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['asset_create']:
                logger.debug('[asset_create] op: {}'.format(op))
                op_data["issuer"] = op[1]['issuer']
                op_data["symbol"] = op[1]['symbol']
                op_data["precision"] = op[1]['precision']
                op_data["max_supply"] = op[1]['common_options']['max_supply']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['create_nh_asset']:
                logger.debug('[nh_asset_create] op: {}'.format(op))
                op_data["owner"] = op[1]['owner']
                op_data["asset_id"] = op[1]['asset_id']
                op_data["world_view"] = op[1]['world_view']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['delete_nh_asset']:
                logger.debug('[nh_asset_delete] op: {}'.format(op))
                op_data["nh_asset"] = op[1]['nh_asset']
                op_data["fee_paying_account"] = op[1]['fee_paying_account']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['fill_nh_asset_order']:
                logger.debug('[fill_nh_asset_order] op: {}'.format(op))
                op_data["order"] = op[1]['order']
                op_data["nh_asset"] = op[1]['nh_asset']
                op_data["price_amount"] = op[1]['price_amount']
                op_data["price_asset_id"] = op[1]['price_asset_id']
                op_data["price_asset_symbol"] = op[1]['price_asset_symbol']
                op_data["op_result"] = tx[1]['operation_results'][op_count]
                operations_list.append(op_data)
            elif op_id == operations['contract_create']:
                contract_name = op[1]['name']
                try:
                    contract_info = gph.rpc.get_contract(contract_name)
                    contract_creater_id = contract_info['owner']
                    contract_creater_name = gph.rpc.get_object(contract_creater_id)['name']
                    op_data["contract_name"] = contract_name
                    op_data["contract_id"] = contract_info['id']
                    op_data["creater"] = contract_creater_name
                    op_data["contract_time"] = contract_info['creation_date']
                    op_data["op_result"] = tx[1]['operation_results'][op_count]
                    operations_list.append(op_data)
                except Exception as e:
                    logger.error("block: {}, trx: {}, op: {}, get_object error: '{}'".format(block_num, transaction_id, op_id, repr(e)))
            elif op_id == operations['call_contract_function']:
                contract_id = op[1]['contract_id']
                contract_caller_id = op[1]['caller']
                contract_caller_name = gph.rpc.get_object(contract_caller_id)["name"]
                function_name = op[1]['function_name']
                value_data = []
                for each in op[1]['value_list']:
                    value_data.append(each[1]['v'])
                contract_info = gph.rpc.get_contract(contract_id)
                contract_name = contract_info['name']
                contract_owner_name = gph.rpc.get_object(contract_info["owner"])["name"]
                token_affected = []
                nht_affected = []
                affected_result = []
                op_results = tx[1]["operation_results"][op_count]
                caller_amount = 0
                owner_amount = 0
                for i in op_results[1]["contract_affecteds"]:
                    if i[0] == 0:
                        affected_account = gph.rpc.get_object(i[1]["affected_account"])["name"]
                        affected_asset = i[1]["affected_asset"]
                        if affected_account == contract_caller_name and affected_asset["asset_id"] == '1.3.0':
                            caller_amount += int(affected_asset["amount"])
                        elif affected_account == contract_owner_name and affected_asset["asset_id"] == '1.3.0':
                            owner_amount += int(affected_asset["amount"])
                        d0 = {"account": affected_account, "asset": affected_asset}
                        token_affected.append(d0)
                    if i[0] == 1:
                        action = i[1]["action"]
                        affected_account = gph.rpc.get_object(i[1]["affected_account"])["name"]
                        affected_item = i[1]["affected_item"]
                        d1 = {"action": action ,"account": affected_account, "item": affected_item}
                        nht_affected.append(d1)
                    if i[0] == 3:
                        affected_account = gph.rpc.get_object(i[1]["affected_account"])["name"]
                        msg = i[1]["message"]
                        d3 = {"account": affected_account, "msg": msg}
                        affected_result.append(d3)
                op_data["contract_name"] = contract_name
                op_data["caller_id"] = contract_caller_id
                op_data["caller_name"] = contract_caller_name
                op_data["function_name"] = function_name
                op_data["value_data"] = value_data
                op_data["token_affected"] = token_affected
                op_data["nht_affected"] = nht_affected
                op_data["affect_result"] = affected_result
                op_data["caller_amount"] = caller_amount,
                op_data["owner_amount"] = owner_amount
                operations_list.append(op_data)
            elif op_id == operations['update_collateral_for_gas']:
                op_data["mortgager"] = op[1]['mortgager']
                op_data["beneficiary"] = op[1]['beneficiary']
                op_data["collateral"] = op[1]['collateral']
                op_data['mortgager_name'] = ""
                op_data['beneficiary_name'] = ""
                try:
                    op_data['mortgager_name'] = gph.rpc.get_object(op_data["mortgager"])['name']
                    op_data['beneficiary_name'] = gph.rpc.get_object(op_data["beneficiary"])['name']
                except Exception as e:
                    logger.error("block: {}, trx: {}, op: {}, get_object error: '{}'".format(block_num, transaction_id, op_id, repr(e)))
                operations_list.append(op_data)
            # else:
            #     operations_list.append({op_id:{}})
            op_count += 1
        
        #transaction
        trx_data = {
            "block_num": block_num, 
            "time": block_time, 
            "op_id": 9999,      # trx identity
            "operation_ids": op_ids,
            "operations_total": len(op_ids),
            "trx_id": transaction_id, 
            "trx_sign": tx[1]["signatures"]
        }
        operations_list.append(trx_data)
    return operations_list

def handle_operations(operations_list):
    if operations_list:
        conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
        conn_db = conn[mongodb_params['db_name']]
        registry = gauges['registry']
        for operation in operations_list:
            op_id = operation["op_id"]
            if op_id == operations['transfer']:
                logger.debug('[transfer] op: {}'.format(operation))
                try: 
                    conn_db.op_transfer.insert_one({'block_num': operation["block_num"], 'time': operation["time"], 'transaction_id': operation["transaction_id"], \
                        'from_account': operation["from"], 'to_account': operation["to"], 'amount_asset_id': operation["amount_asset_id"], 'amount_amount': operation["amount_amount"], \
                        'op_result': str(operation["op_result"]) })
                except Exception as e:
                    logger.error("op_transfer_mongodb_error >>>:{}---'{}'".format(operation["block_num"], repr(e)))
            elif op_id == operations['account_create']:
                logger.debug('[create account] op: {}'.format(operation))
                try: 
                    conn_db.op_account_create.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'account_name': operation["account_name"], 
                        'op_result': str(operation["op_result"]) 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                op_result = operation["op_result"]
                new_account_id = op_result[1]['result']
                if new_account_id.startswith('1.2.') :
                    tokens = new_account_id.split('.')
                    try:
                        gauges[op_id].labels(monitor_env, operation["block_num"], operation["transaction_id"], operation["account_name"], new_account_id, operation["time"], op_id).set(int(tokens[2]))
                        pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                        gauges[op_id].remove(monitor_env, operation["block_num"], operation["transaction_id"], operation["account_name"], new_account_id, operation["time"], op_id)
                    except Exception as e:
                        logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
                else:
                    logger.error('[op_account_create] op result error, account_id: {}'.format(new_account_id))
            elif op_id == operations['asset_create']:
                op_result = operation["op_result"]
                asset_id = op_result[1]['result']
                logger.debug('[asset create] op: {}'.format(operation))
                try: 
                    conn_db.op_asset_create.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'issuer': operation["issuer"], 
                        'symbol': operation['symbol'], 
                        'precision': operation['precision'], 
                        'max_supply': operation['max_supply'],
                        'op_result': str(op_result), 
                        'asset_id': asset_id 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                if asset_id.startswith('1.3.'):
                    tokens = asset_id.split('.') #total = int(tokens[2])
                    try:
                        gauges[op_id].labels(monitor_env, operation["block_num"], operation["transaction_id"], operation["issuer"], operation['symbol'], operation['precision'], \
                        operation['max_supply'], asset_id, operation["time"], op_id).set(int(tokens[2]))
                        pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                        gauges[op_id].remove(monitor_env, operation["block_num"], operation["transaction_id"], operation["issuer"], operation['symbol'], operation['precision'], \
                        operation['max_supply'], asset_id, operation["time"], op_id)
                    except Exception as e:
                        logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
                else:
                    logger.error('[op_asset_create] op result error, account_id: {}'.format(new_account_id))
            elif op_id == operations['fill_nh_asset_order']:
                logger.debug('[op_nh_asset_order_fill] op: {}'.format(operation))
                try: 
                    conn_db.op_nh_asset_order_fill.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'order': operation["order"],
                        'nh_asset': operation['nh_asset'], 
                        'price': operation['price_amount'], 
                        'price_asset': operation['price_asset_id'], 
                        'price_asset_symbol': operation['price_asset_symbol'],
                        'op_result': str(operation["op_result"]) 
                    })
                    res_count = conn_db.op_nh_asset_order_fill.count()
                    logger.debug('[op_nh_asset_order_fill] res_count: {}'.format(res_count))
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                try:
                    gauges[op_id].labels(monitor_env, operation["block_num"], operation["transaction_id"], operation["order"], operation['nh_asset'], operation['price_amount'], \
                    operation['price_asset_id'], operation['price_asset_symbol'], operation["time"], op_id).set(res_count)
                    pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                    gauges[op_id].remove(monitor_env, operation["block_num"], operation["transaction_id"], operation["order"], operation['nh_asset'], operation['price_amount'], \
                    operation['price_asset_id'], operation['price_asset_symbol'], operation["time"], op_id)
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
            elif op_id == operations['create_nh_asset']:
                op_result = operation["op_result"]
                nh_asset_id = op_result[1]['result']
                logger.debug('[nh asset create] op: {}'.format(operation))
                try: 
                    conn_db.op_nh_asset_create.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'owner': operation["owner"], 
                        'asset_id': operation['asset_id'], 
                        'world_view': operation['world_view'], 
                        'op_result': str(op_result), 
                        'nh_asset_id': nh_asset_id 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                if nh_asset_id.startswith('4.2.') :
                    tokens = nh_asset_id.split('.') #total = int(tokens[2])
                    delete_count = conn_db.op_nh_asset_delete.count()
                    total = int(tokens[2]) - delete_count + 1
                    try:
                        gauges[op_id].labels(monitor_env, operation["block_num"], operation["transaction_id"], operation["owner"], operation['asset_id'], \
                        operation['world_view'], nh_asset_id, operation["time"], op_id).set(total)
                        pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                        gauges[op_id].remove(monitor_env, operation["block_num"], operation["transaction_id"], operation["owner"], operation['asset_id'], \
                        operation['world_view'], nh_asset_id, operation["time"], op_id)
                    except Exception as e:
                        logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
                else:
                    logger.error('[op_nh_asset_create] op result error, account_id: {}'.format(new_account_id))
            elif op_id == operations['delete_nh_asset']:
                logger.debug('[nh asset delete] op: {}'.format(operation))
                try: 
                    conn_db.op_nh_asset_delete.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'nh_asset': operation["nh_asset"], 
                        'fee_paying_account': operation['fee_paying_account'], 
                        'op_result': str(operation["op_result"]) 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                create_count = conn_db.op_nh_asset_create.count()
                delete_count = conn_db.op_nh_asset_delete.count()
                total = create_count - delete_count + 1
                try:
                    gauges[operations['create_nh_asset']].labels(monitor_env, operation["block_num"], operation["transaction_id"], "", "", "", operation['nh_asset'], operation["time"], op_id).set(total)
                    pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                    gauges[operations['create_nh_asset']].remove(monitor_env, operation["block_num"], operation["transaction_id"], "", "", "", operation['nh_asset'], operation["time"], op_id)
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
            elif op_id == operations['contract_create']:
                try: 
                    conn_db.op_contract_create.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'contract_name': operation["contract_name"], 
                        'contract_id': operation["contract_id"], 
                        'contract_creater': operation["creater"], \
                        'contract_time': operation["contract_time"], 
                        'op_result': str(operation["op_result"]) 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
                op_result = operation["op_result"]
                contract_id = op_result[1]['result']
                if contract_id.startswith('1.16.') :
                    tokens = contract_id.split('.')
                    try:
                        gauges[op_id].labels(monitor_env, operation["block_num"], operation["transaction_id"], operation["contract_name"], contract_id, operation["time"], operation["creater"], op_id).set(int(tokens[2]))
                        pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                        gauges[op_id].remove(monitor_env, operation["block_num"], operation["transaction_id"], operation["contract_name"], contract_id, operation["time"], operation["creater"], op_id)
                    except Exception as e:
                        logger.error("block num: {}, op_id: {}, push except: '{}'".format(operation["block_num"], op_id, repr(e)))
                else:
                    logger.error('[op_contract_create] op result error, account_id: {}'.format(contract_id))
            elif op_id == operations['call_contract_function']:
                logger.debug('[call contract] op: {}'.format(operation))
                try: 
                    conn_db.op_contract_call.insert_one({
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"],
                        'contract_name': operation["contract_name"], 
                        'caller_id': operation["caller_id"], 
                        'caller_name': operation["caller_name"], 
                        'function_name': operation["function_name"], 
                        'value_data': operation["value_data"], 
                        'token_affected': str(operation["token_affected"]),
                        'nht_affected': str(operation["nht_affected"]), 
                        'affect_result': str(operation["affect_result"]), 
                        'caller_amount': operation["caller_amount"], 
                        'owner_amount': operation["owner_amount"] 
                    })
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
            elif op_id == operations['update_collateral_for_gas']:
                logger.debug('[update_collateral] op: {}'.format(operation))
                mortgager_id = operation["mortgager"]
                beneficiary_id = operation["beneficiary"]
                try: 
                    update_collateral_info = {
                        'block_num': operation["block_num"], 
                        'time': operation["time"], 
                        'transaction_id': operation["transaction_id"], 
                        'mortgager': mortgager_id, 
                        'beneficiary': beneficiary_id, 
                        'collateral': operation["collateral"],
                        'mortgager_name': operation['mortgager_name'],
                        'beneficiary_name': operation['beneficiary_name']
                    }
                    conn_db.op_update_collateral.insert(update_collateral_info)

                    if operation["collateral"] > 0:
                        result_obj = conn_db.account_collateral.find_one({'mortgager':mortgager_id, 'beneficiary':beneficiary_id})
                        if (result_obj is not None) and (operation["block_num"] >= result_obj["block_num"]):
                            logger.debug('collateral({}->{}) already exists in mongodb. block num({} -> {})'.format(mortgager_id, beneficiary_id, result_obj["block_num"], operation["block_num"])) 
                            update_collateral_info['_id'] = result_obj['_id']
                        update_collateral_info['mortgager_name'] = operation['mortgager_name']
                        update_collateral_info['beneficiary_name'] = operation['beneficiary_name']
                        conn_db.account_collateral.save(update_collateral_info)
                    else:
                        conn_db.account_collateral.remove({'mortgager':mortgager_id, 'beneficiary':beneficiary_id})
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
            elif op_id == 9999:
                try: 
                    conn_db.transaction.insert_one(operation)
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, trx db except: '{}'".format(operation["block_num"], op_id, repr(e)))
            
                try:
                    gauges[op_id].labels(monitor_env, operation["block_num"], operation["trx_id"], operation["time"], operation["operation_ids"], operation["operations_total"]).set(1)
                    pushadd_to_gateway(prometheus_addr, job=gateway_job, registry=registry)
                    gauges[op_id].remove(monitor_env, operation["block_num"], operation["trx_id"], operation["time"], operation["operation_ids"], operation["operations_total"])
                except Exception as e:
                    logger.error("block num: {}, trx_id: {}, push trx except: '{}'".format(operation["block_num"], operation["trx_id"], repr(e)))
        conn.close()
          
    #return True

