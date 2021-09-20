package odysseus4iot.model;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.model.management.ModelManagementModel;
import odysseus4iot.model.management.ModelManagementModelset;
import odysseus4iot.model.management.ModelManagementRequest;
import odysseus4iot.model.management.ModelManagementRequestData;
import odysseus4iot.model.management.ModelManagementResponse;
import odysseus4iot.util.Util;

//sensor system -> sensor_system (input)
//window.type_size_stride -> split into window_size and window_stride (remove type and no point in name)
//What is the sense of boolean type and number of sensor systems?
public class ModelManagementImport
{
	public static String host = null;
	public static String port = null;
	public static String user = null;
	public static String password = null;
	
	public static List<List<Model>> importFromModelManagementSystem()
	{
		List<List<Model>> modelsets = new ArrayList<>();
		
		ModelManagementRequestData modelManagementRequestData = new ModelManagementRequestData();
		modelManagementRequestData.sensor_system = "Blaupunkt_BST-BNO055-DS000-14_NDOF_10_AO";
		modelManagementRequestData.addLabel("Stehen", 90.97d);
		modelManagementRequestData.addLabel("Grasen", 95.22d);
		modelManagementRequestData.maxsizeofbignode = 0;
		modelManagementRequestData.maxsizeof_ALL = 0;
		modelManagementRequestData.modelset_limit = 1;
		
		ModelManagementRequest modelManagementRequest = new ModelManagementRequest();
		modelManagementRequest.request = modelManagementRequestData;
		
		System.out.println(Util.toJson(modelManagementRequest));
		
		//TODO: ___ send request
		
		String response = "{\r\n"
				+ "    \"response\": [\r\n"
				+ "        {\r\n"
				+ "            \"Modelset_name\": \"6892\",\r\n"
				+ "            \"avg_accuracy\": 0.9555,\r\n"
				+ "            \"query_sharing_level\": 3,\r\n"
				+ "            \"final_score\": 0.9555,\r\n"
				+ "            \"number_of_models\": 1,\r\n"
				+ "            \"ml_models\": [\r\n"
				+ "                {\r\n"
				+ "                    \"model_id\": 6892,\r\n"
				+ "                    \"type\": false,\r\n"
				+ "                    \"labels\": [\r\n"
				+ "                        \"Grasen\",\r\n"
				+ "                        \"Stehen\",\r\n"
				+ "                        \"Wiederkauen\"\r\n"
				+ "                    ],\r\n"
				+ "                    \"accuracy\": 0.9555,\r\n"
				+ "                    \"sensor_system\": 1,\r\n"
				+ "                    \"window_type\": \"Jumping\",\r\n"
				+ "                    \"window_size\": \"12000\",\r\n"
				+ "                    \"window_stride\": \"100\",\r\n"
				+ "                    \"features\": {\r\n"
				+ "                        \"accMag\": [\r\n"
				+ "                            {\r\n"
				+ "                                \"function\": \"median\",\r\n"
				+ "                                \"type\": \"double\",\r\n"
				+ "                                \"order\": 0\r\n"
				+ "                            }\r\n"
				+ "                        ]\r\n"
				+ "                    },\r\n"
				+ "                    \"size\": 15152,\r\n"
				+ "                    \"algorithm\": \"SVM\"\r\n"
				+ "                }\r\n"
				+ "            ]\r\n"
				+ "        }]}\r\n";
		
		ModelManagementResponse modelManagementResponse = Util.fromJson(response, ModelManagementResponse.class);
		
		ModelManagementModelset modelManagementModelset = null;
		
		ModelManagementModel modelManagementModel = null;
		
		List<Model> modelset = null;
		
		System.out.println(Util.toJson(modelManagementResponse));
		
		List<ModelManagementModelset> modelManagementModelsets = modelManagementResponse.response;
		
		for(int index = 0; index < modelManagementModelsets.size(); index++)
		{
			modelManagementModelset = modelManagementModelsets.get(index);
			
			modelset = new ArrayList<>();
			
			for(int index2 = 0; index2 < modelManagementModelset.ml_models.size(); index2++)
			{
				modelManagementModel = modelManagementModelset.ml_models.get(index2);
				
				modelset.add(modelManagementModel.toModel());
			}
			
			modelsets.add(modelset);
		}
		
		return modelsets;
	}
}