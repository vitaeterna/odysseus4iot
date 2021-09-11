package odysseus4iot.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PostgresImport
{
	public static Properties properties = null;
	
	public static String url = null;
	public static String user = null;
	public static String password = null;
	
	public static String SQL_QUERY = 
			  "SELECT model_title, features_json_content, list_of_predicted_classes, resampled_rate_in_hz, algorithm, list_of_functions, list_of_axes, window_size, window_stride, accuracy_test, f1_test\r\n"
			+ "FROM public.experiment_result\r\n"
			+ "WHERE model_title <> ''\r\n"
			+ "AND algorithm LIKE '%Random%Forest%'\r\n"
			+ "AND window_size = 5000\r\n"
			+ "AND window_stride = '100%'\r\n"
			+ "AND list_of_axes = 'gyrMag_accMag'\r\n"
			+ "AND accuracy_test >= 0.9\r\n"
			+ "AND f1_test >= 0.9\r\n"
			+ "AND (model_title = '3Cattle1FarmRF_Lying' OR model_title = '3Cattle1FarmRF_Grassing' OR model_title = '3Cattle1FarmRF_Ruminating');";
	
	public static List<Model> importFromDB()
	{
		Properties dbProperties = new Properties();
		dbProperties.setProperty("user",user);
		dbProperties.setProperty("password",password);
		
		List<Model> models = new ArrayList<>();
		
		try
		{
			Connection connection = DriverManager.getConnection(url, dbProperties);
			connection.setAutoCommit(true);
			
			PreparedStatement preparedStatement = connection.prepareStatement(SQL_QUERY);
			//preparedStatement.setInt(1, foovalue);
			
			preparedStatement.setFetchSize(0);
			
			ResultSet resultSet = preparedStatement.executeQuery();
			
			Model model = null;
			
			while (resultSet.next())
			{
				model = new Model();
				
				model.setModel_title(resultSet.getString(1));
				model.setFeatures_json_content(resultSet.getString(2));
				model.setList_of_predicted_classes(resultSet.getString(3));
				model.setResampled_rate_in_hz(resultSet.getInt(4));
				model.setAlgorithm(resultSet.getString(5));
				model.setList_of_functions(resultSet.getString(6));
				model.setList_of_axes(resultSet.getString(7));
				model.setWindow_size(resultSet.getInt(8));
				model.setWindow_stride(resultSet.getString(9));
				model.setAccuracy_test(resultSet.getDouble(10));
				model.setF1_test(resultSet.getDouble(11));
				
				List<String> schema = new ArrayList<>();
				List<String> preprocessing = new ArrayList<>();
				List<String> features = new ArrayList<>();
				
				JsonElement jsonElement = JsonParser.parseString(model.getFeatures_json_content());
				JsonObject jsonObject = jsonElement.getAsJsonObject();
				Set<Entry<String, JsonElement>> entrySet = jsonObject.entrySet();
				
				String currentKey = null;
				JsonArray currentValue = null;
				
				for(Entry<String, JsonElement> entry : entrySet)
				{
					currentKey = entry.getKey().toLowerCase();
					currentValue = entry.getValue().getAsJsonArray();
					
					//Schema
					if(properties.getProperty("schema." + currentKey) == null)
					{
						System.err.println("The schema property 'schema." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					List<String> schemaElements = Arrays.asList(properties.getProperty("schema." + currentKey).split(","));
					
					String currentSchemaElement = null;
					
					for(int index = 0; index < schemaElements.size(); index++)
					{
						currentSchemaElement = schemaElements.get(index).toLowerCase();
						
						if(!schema.contains(currentSchemaElement))
						{
							schema.add(currentSchemaElement);
						}
					}
					
					//Preprocessing
					if(properties.getProperty("preprocessing." + currentKey) == null)
					{
						System.err.println("The schema property 'preprocessing." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					String preprocessingMapping = properties.getProperty("preprocessing." + currentKey);
					
					if(!preprocessing.contains(preprocessingMapping))
					{
						preprocessing.add(preprocessingMapping);
					}
					
					//Features
					//TODO: proper sorting of features
					String currentSubValue = null;
					
					for(int index = 0; index < currentValue.size(); index++)
					{
						currentSubValue = currentValue.get(index).getAsString().toLowerCase();
						
						features.add(currentKey + "_" + currentSubValue);
					}
				}
				
				Collections.sort(schema);
				Collections.sort(preprocessing);
				Collections.sort(features);
				
				model.setSchema(schema);
				model.setPreprocessing(preprocessing);
				model.setFeatures(features);
				
				models.add(model);
			}
			
			resultSet.close();
			preparedStatement.close();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		
		return models;
	}
}