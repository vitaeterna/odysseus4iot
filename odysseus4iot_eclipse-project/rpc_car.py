import json
import logging
import pickle
import pandas
import psycopg2
import re
import sklearn
import sys
from psycopg2 import sql
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

myLogger = logging.getLogger()
myLogger.setLevel(logging.DEBUG)

fileLogger = logging.FileHandler("fileLog.log")
fileLogger.setLevel(logging.DEBUG)
fileLogger.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(message)s",datefmt="%H:%M:%S"))
myLogger.addHandler(fileLogger)

consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(logging.Formatter("%(message)s"))
myLogger.addHandler(consoleLogger)

rpcHost = 'localhost'
rpcPort = 9000
rpcPath = '/rpc'

loadedModelName = None;
loadedModel = None;

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = (rpcPath,)

# Create server
with SimpleXMLRPCServer((rpcHost, rpcPort), requestHandler=RequestHandler) as server:
    server.register_introspection_functions()

    # Register a function under a different name
    def predict(dbPropertiesJson, sensorDataJson):
        try:
            logging.debug("dbPropertiesJson:\n"+dbPropertiesJson)
            logging.debug("sensorDataJson:\n"+sensorDataJson)

            dbPropertiesDict = json.loads(dbPropertiesJson)

            logging.debug("dbPropertiesDict:\n"+str(dbPropertiesDict))

        except JSONDecodeError:
            logging.error("Exception occurred while trying to"
                          " read database properties :" + str(dbPropertiesJson))
            logging.error(sys.exc_info())

        host = dbPropertiesDict["host"]
        port = dbPropertiesDict["port"]
        database = dbPropertiesDict["database"]
        username = dbPropertiesDict["username"]
        password = dbPropertiesDict["password"]
        table = dbPropertiesDict["table"]
        column = dbPropertiesDict["selectModelByColumn"]
        modelid = dbPropertiesDict["selectModelByValue"]

        params_db = {'host': host, 'port': port, 'database': database,'user': username, 'password': password}

        try:
            global loadedModelName
            global loadedModel
            if not (loadedModelName == modelid):
                logging.info("Loading new model id=" + str(modelid) + " from " + host + ":" + port + "/" + database + "/" + table + "/" + column)
                conn = psycopg2.connect(**params_db)
                cursor = conn.cursor()
                query = sql.SQL("SELECT {column} FROM {table} WHERE id = %s").format(column=sql.Identifier(column),table=sql.Identifier(table))
                
                logging.debug("SQL query:\n"+str(query))
                
                cursor.execute(query, (modelid,))
                model_retrieved = cursor.fetchone()

                if cursor.rowcount == 0:
                    logging.error("No model with the given id is found on the database!")
                else:
                    loadedModel = pickle.loads(model_retrieved[0])
                    loadedModelName = modelid

                    logging.info("Model successfully loaded! id=" + str(modelid) + " " + str(loadedModel) + " #features=" + str(loadedModel.n_features_in_))
            else:
                logging.info("Using existing model! id=" + str(modelid) + " " + str(loadedModel) + " #features=" + str(loadedModel.n_features_in_))
        except:
            logging.error("Exception occurred while trying to"
                          " load model from database :")
            logging.error(sys.exc_info())

        try:
            sensorDataDict = json.loads(sensorDataJson)
            sensorDataDict = dict(sorted(sensorDataDict.items()))

            logging.debug("sensorDataDict:\n"+str(sensorDataDict))

        except:
            logging.error("Exception occurred while trying to"
                          " load sensor data :")
            logging.error(sys.exc_info())

        featureDict = {}
        for key in sensorDataDict:
            if key.endswith('_car'):
                featureDict[key] = [sensorDataDict[key]]

        logging.debug("featureDict:\n"+str(featureDict))
        logging.debug("The size of the feature set is "+str(len(featureDict)))

        try:
            dataFrame = pandas.DataFrame(data=featureDict)
            logging.debug("Pandas Dataframe:\n"+str(dataFrame))
            logging.debug("Pandas Dataframe Data Types:\n"+str(dataFrame.dtypes))
        except:
            logging.error("Exception occurred while trying to"
                          " create data frame :")
            logging.error(sys.exc_info())

        try:
            if loadedModel is not None:
                result = loadedModel.predict(dataFrame)
                #logging.info("prediction : "+str(result)+" "+str(type(result)))      # -> <class 'numpy.ndarray'>
                logging.info("prediction : "+str(result[0]))                          # -> <class 'numpy.str_'>
                return str(result[0])
            else:
                logging.error("Model is undefined!")
                logging.error(sys.exc_info())
        except ValueError:
            logging.error("could not predict label")
            logging.error(sys.exc_info())

        return "error"

    server.register_function(predict, 'predict')

    logging.info("RPC server started at url "+rpcHost+":"+str(rpcPort)+rpcPath)

    # Run the server's main loop
    server.serve_forever()