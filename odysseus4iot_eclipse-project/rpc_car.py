import json
import logging
import pickle
import re
import sys
from json import JSONDecodeError
import sklearn
import pandas as pd
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
import psycopg2

logging.basicConfig(filename="mylog.log",level=logging.INFO,format='%(asctime)s :: %(levelname)s :: %(message)s')

rpcPort = 9000
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/rpc',)


# Create server
with SimpleXMLRPCServer(('', rpcPort),
                        requestHandler=RequestHandler) as server:
    loadedModelName = "";
    loadedModel = any;
    server.register_introspection_functions()

    # Register a function under a different name
    def predict(dbPropertiesJson, sensorDataJson):
        try:
            arg = re.sub("([\w-]+):", r'"\1":', dbPropertiesJson)
            json_val = json.loads(arg)
            logging.info("Database properties loaded :" + str(json_val['database']
                                                                 + " " + json_val['host']
                                                                 + " " + json_val['port']
                                                                 + " " + json_val['table']))
        except JSONDecodeError:
            logging.error("Exception occurred while trying to"
                          " read database properties :" + str(dbPropertiesJson))
            logging.error(sys.exc_info())

        params_db = {'host': json_val["host"], 'port': json_val["port"], 'database': json_val["database"],
                     'user': json_val["username"], 'password': json_val["password"]}
        trained_models_table = json_val["table"]

        try:
            global loadedModelName, loadedModel
            if not (loadedModelName == json_val["selectModelByValue"]):
                logging.info("Loading new model " + str(json_val["selectModelByValue"]))
                conn = psycopg2.connect(**params_db)
                cur = conn.cursor()
                # Connect to the table model to get meta-data =>
                # 1. Get the model pickle content
                sql_fetch_model_content = 'SELECT model_binary_content FROM ' + trained_models_table + ' WHERE model_title = %s '
                cur.execute(sql_fetch_model_content, (json_val["selectModelByValue"],))
                model_retrieved = cur.fetchone()
                if cur.rowcount == 0:
                    # No model with the given  name is found
                    logging.error("No model with the given name is found on the database!")
                else:
                    loadedModel = pickle.loads(model_retrieved[0])
                    loadedModelName = json_val["selectModelByValue"]
            else:
                logging.info("Using existing model " + str(json_val["selectModelByValue"]))
        except:
            logging.error("Exception occurred while trying to"
                          " load model from database :")
            logging.error(sys.exc_info())

        try:
            arg1 = re.sub("([\w-]+):", r'"\1":', sensorDataJson)
            arg1 = arg1.replace("_", "-")
            sensorJson = json.loads(arg1)
        except:
            logging.error("Exception occurred while trying to"
                          " load sensor data :")
            logging.error(sys.exc_info())

        data = {}
        for signal in sensorJson:
            #if not (signal == 'cattle-id') and not (signal == 'round') and not (signal == 'count'):
            if signal.startswith('car-'):
                data[signal] = [sensorJson[signal]]
        logging.info("The length of the input data is "+str(len(data)))
        print("The length of the input data is "+str(len(data)))
        try:
            df = pd.DataFrame(data, columns=data.keys())
            for col in data.keys():
                df[col] = df[col].astype(float)
        except:
            logging.error("Exception occurred while trying to"
                          " create data frame :")
            logging.error(sys.exc_info())
        try:
            if len(loadedModel) > 0:
                
                logging.info("The number of features of the loaded model is "+str(loadedModel.n_features_))
                print("The number of features of the loaded model is "+str(loadedModel.n_features_))
                res = loadedModel.predict(df)
                logging.info("prediction : "+res)
                return str(res)
            else:
                logging.error("did not receive model")
                logging.error(sys.exc_info())
        except ValueError:
            logging.error("could not predict label")
            logging.error(sys.exc_info())
        return "error"


    server.register_function(predict, 'predict')
    logging.info("function predict published in port "+str(rpcPort))
    print("function predict published in port "+str(rpcPort))

    # Run the server's main loop
    server.serve_forever()
