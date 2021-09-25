package odysseus4iot.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Vector;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

import com.google.gson.JsonObject;

public class RPCTestMain
{
	//XMLRPC Properties
	public static String rpcServerString = "localhost:9000";
	public static Integer connectionTimeout = 10000;
	
	//DB Properties
	public static String host = "141.13.162.179";
	public static String port = "5432";
	public static String database = "CARModels";
	public static String username = "script";
	public static String password = "pAhXHnnFf6jgxO85";
	public static String table = "trained_models";
	public static String selectModelByColumn = "model_content";
	public static String selectModelByValue = "4585";
	
	public static void main(String[] args) throws MalformedURLException, XmlRpcException
	{
		JsonObject dbProperties = new JsonObject();
		dbProperties.addProperty("host", host);
		dbProperties.addProperty("port", port);
		dbProperties.addProperty("database", database);
		dbProperties.addProperty("username", username);
		dbProperties.addProperty("password", password);
		dbProperties.addProperty("table", table);
		dbProperties.addProperty("selectModelByColumn", selectModelByColumn);
		dbProperties.addProperty("selectModelByValue", selectModelByValue);
		
		JsonObject sensorDataJson = new JsonObject();
		sensorDataJson.addProperty("cattle_id", 19);
		sensorDataJson.addProperty(String.format("%03d", 333) + "_pi_car", Math.PI);
		sensorDataJson.addProperty(String.format("%03d", 16) + "_pi_car", Math.PI);
		sensorDataJson.addProperty(String.format("%03d", 160) + "_pi_car", Math.PI);
		sensorDataJson.addProperty(String.format("%03d", 332) + "_pi_car", Math.PI);
		sensorDataJson.addProperty(String.format("%03d", 15) + "_pi_car", Math.PI);
		sensorDataJson.addProperty(String.format("%03d", 166) + "_pi_car", Math.PI);
		
		XmlRpcClientConfigImpl cf = new XmlRpcClientConfigImpl();
		cf.setServerURL(new URL("http://"+rpcServerString+"/rpc"));
		cf.setConnectionTimeout(connectionTimeout);

		System.out.println("accessing rpc at url : "+cf.getServerURL());

		XmlRpcClient client = new XmlRpcClient();
		client.setConfig(cf);

		Vector<String> params = new Vector<>();
		params.add(dbProperties.toString());
		params.add(sensorDataJson.toString());

		Object result = client.execute("predict", params);

		String predictionResult = ((String) result);
		
		System.out.println(predictionResult);
	}
}