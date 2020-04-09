
import pymongo
from config import *
from utils import Logging

logger = Logging().getLogger()

conn = pymongo.MongoClient(mongodb_params['host'], mongodb_params['port'])
conn_db = conn[mongodb_params['db_name']]
count = 1
for object in conn_db.account_collateral.find():
    logger.info(object)
    object["collateral"] = str(object["collateral"])
    logger.info(object)
    logger.info("--------------- {} ---------------------\n".format(count))
    count += 1
    # conn_db.account_collateral.save(object)

