import os
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from utils.ServerLogger import ServerLogger

logger = ServerLogger()


class MongoCore:

    instance_details = {}

    mongo_uri = os.environ.get("MONGO_CONNECTION")
    __db_connection = None
    __db = None

    def __init__(self, **kwargs):
        self.instance_details = {**self.instance_details, **kwargs}
        logger.info(f"{logger.doc} connected to {self.instance_details['database']}")
        if kwargs.get("async-client"):
            logger.warn(f"{logger.WIP} Initializing async client")
            # Logic: Enable TLS only if not on localhost, or if explicitly requested
            use_tls = "localhost" not in self.mongo_uri and "127.0.0.1" not in self.mongo_uri
            self.__db_connection = AsyncIOMotorClient(
                self.mongo_uri, tls=use_tls, tlsAllowInvalidCertificates=True
            )
        else:
            use_tls = "localhost" not in self.mongo_uri and "127.0.0.1" not in self.mongo_uri
            self.__db_connection = MongoClient(
                self.mongo_uri, tls=use_tls, tlsAllowInvalidCertificates=True
            )
        assert kwargs["database"]
        database = kwargs["database"]
        self.__db = self.__db_connection[database]

    def get_collection(
        self,
        collection_name: str,
    ):
        return self.__db[collection_name]


monet_db = MongoCore(database="diy_monet")
monet_db_test = MongoCore(database="diy_monet_test")
