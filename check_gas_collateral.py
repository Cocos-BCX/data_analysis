# -*- coding:utf-8 -*-
from PythonMiddleware.notify import Notify
from PythonMiddleware.graphene import Graphene
from PythonMiddlewarebase.operationids import operations

import sys
import pymongo
import datetime
from time import sleep
from collections import deque
from threading import Thread, Lock

from config import *
from utils import Logging

operations_deque = deque() 
op_deque_lock = Lock()

collateral_dict = {} 

logger = Logging().getLogger()

def check_block(args):
    start = args[0]
    end = args[1]
    gph = Graphene(node=nodeaddress)
    info = gph.info()
    last_block_num = info['head_block_number']
    logger.info('last_block_num: {}, block start: {}, end: {}'.format(last_block_num, start, end))
    if start > last_block_num:
        logger.error("start:{} < end:{}".format(start, end))
        return
    if end > last_block_num:
        end = last_block_num
    for block_num in range(end, start, -1):
        try:
            block_info = gph.rpc.get_block(block_num)
            operations_list = parse_gas_collateral_operations(gph, block_num, block_info["timestamp"], block_info["transactions"])
            if operations_list:
                op_deque_lock.acquire()
                operations_deque.append(operations_list)
                op_deque_lock.release()
        except Exception as e:
            logger.error('parse block exception. block {}, error {}'.format(block_num, repr(e)))

def parse_gas_collateral_operations(gph, block_num, block_time, transactions):
    operations_list = []
    for tx in transactions:
        transaction_id = tx[0]
        for op in tx[1]['operations']:
            op_id = op[0]
            op_data = {"block_num": block_num, "time": block_time, "transaction_id": transaction_id, "op_id": op_id}
            if op_id == operations['update_collateral_for_gas']:
                mortgager = op[1]['mortgager']
                beneficiary = op[1]['beneficiary']
                if mortgager in collateral_dict:
                    beneficiary_list = collateral_dict[mortgager] 
                    if beneficiary in beneficiary_list:
                        continue
                    else:
                        beneficiary_list.append(beneficiary)
                        collateral_dict[mortgager] = beneficiary_list
                else:
                    collateral_dict[mortgager] = [beneficiary]
                op_data["mortgager"] = mortgager
                op_data["beneficiary"] = beneficiary
                op_data["collateral"] = op[1]['collateral']
                op_data['mortgager_name'] = ""
                op_data['beneficiary_name'] = ""
                try:
                    op_data['mortgager_name'] = gph.rpc.get_object(op_data["mortgager"])['name']
                    if op_data["mortgager"] == op_data["beneficiary"]:
                        op_data['beneficiary_name'] = op_data['mortgager_name']
                    else:
                        op_data['beneficiary_name'] = gph.rpc.get_object(op_data["beneficiary"])['name']
                except Exception as e:
                    logger.error("block: {}, trx: {}, op: {}, get_object error: '{}'".format(block_num, transaction_id, op_id, repr(e)))
                operations_list.append(op_data)
    return operations_list

def data2db(): 
    while 1:
        if operations_deque:
            try:
                op_deque_lock.acquire()
                operations_list = operations_deque.popleft()
                op_deque_lock.release()
                logger.debug(operations_list)
                handle_gas_collateral_operations(operations_list)
            except Exception as e:
                logger.error("except: '{}'".format(repr(e)))
        sleep(0.5)

def handle_gas_collateral_operations(operations_list):
    if operations_list:
        conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
        conn_db = conn[mongodb_params['db_name']]
        #registry = gauges['registry']
        for operation in operations_list:
            op_id = operation["op_id"]
            if op_id == operations['update_collateral_for_gas']:
                logger.debug('[update_collateral] op: {}'.format(operation))
                mortgager_id = operation["mortgager"]
                beneficiary_id = operation["beneficiary"]
                try: 
                    # result = conn_db.block.find({'block_num':index})
                    # result = conn_db.account_collateral.find({"beneficiary":beneficiary_id, "mortgager": mortgager_id})
                    # if result.count() == 0:
                    #     logger.info('check block number: {}'.format(index))

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
                    #conn_db.op_update_collateral.insert(update_collateral_info)

                    collateral = int(operation["collateral"])
                    if collateral > 0:
                        result_obj = conn_db.account_collateral.find_one({'mortgager':mortgager_id, 'beneficiary':beneficiary_id})
                        if (result_obj is not None) and (operation["block_num"] >= result_obj["block_num"]):
                            logger.debug('collateral({}->{}) already exists in mongodb. block num({} -> {})'.format(mortgager_id, beneficiary_id, result_obj["block_num"], operation["block_num"])) 
                            update_collateral_info['_id'] = result_obj['_id']
                        update_collateral_info['mortgager_name'] = operation['mortgager_name']
                        update_collateral_info['beneficiary_name'] = operation['beneficiary_name']
                        update_collateral_info['collateral'] = collateral
                        conn_db.account_collateral.save(update_collateral_info)
                    else:
                        conn_db.account_collateral.remove({'mortgager':mortgager_id, 'beneficiary':beneficiary_id})
                except Exception as e:
                    logger.error("block num: {}, op_id: {}, db except: '{}'".format(operation["block_num"], op_id, repr(e)))
        conn.close()

if __name__ == '__main__':
    logger.info("=================== check collateral gas start ====================")
    logger.info('args: {}'.format(sys.argv))
    if len(sys.argv) < 3:
        logger.error('Usage: python3 check_gas_collateral.py block_number_start, block_number_end')
        sys.exit(1)
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    if start > end or start <= 0 or end <= 0:
        logger.error('block_number_start: {} > block_number_end: {} or start <= 0 or end <= 0'.format(start, end))
        sys.exit(1)
    args = [start, end]
    t1 = Thread(target=check_block, args=(args,))
    t1.start()
    t2 = Thread(target=data2db)
    t2.start()
