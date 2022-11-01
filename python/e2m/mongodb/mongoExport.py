import pymongo


class MongoExport:

    def __init__(self, database, collection, clear=False, host=None, port=None, user=None, password=None):
        self.__host = host or "localhost"
        self.__port = port or 27019
        self.__user = user
        self.__password = password
        self.__usrPsrStr = "" if not user and not password else f"{user}:{password}@"
        self.__mongoUri = f"mongodb://{self.__usrPsrStr}{self.__host}:{self.__port}"
        self.__mongoClient = pymongo.MongoClient(self.__mongoUri)
        self.__dbase = self.__mongoClient.get_database(database)
        self.__clear = self.__dbase.get_collection(collection) if not clear else {
            self.drop_coll(self.__dbase.get_collection(collection))
        }
        self.__coll = self.__dbase.get_collection(collection)

    def insert(self, document):
        self.__coll.insert_one(document)

    def close(self):
        self.__mongoClient.close()

    @staticmethod
    def drop_coll(collection):
        collection.drop()
        return collection
