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

import odysseus4iot.main.Main;

public class PostgresImport
{
	public static String url = null;
	public static String table = null;
	public static String user = null;
	public static String password = null;
	
	public static String SQL_QUERY_EXPERIMENTRESULT = 
			  "SELECT model_title, features_json_content, list_of_predicted_classes, resampled_rate_in_hz, algorithm, list_of_functions, list_of_axes, window_size, window_stride, accuracy_test, f1_test\r\n"
			+ "FROM public.%s\r\n"
			+ "WHERE model_title <> ''\r\n"
			+ "AND algorithm LIKE '%%Random%%Forest%%'\r\n"
			+ "AND window_size = 5000\r\n"
			+ "AND window_stride = '100%%'\r\n"
			+ "AND list_of_axes = 'gyrMag_accMag'\r\n"
			+ "AND accuracy_test >= 0.9\r\n"
			+ "AND f1_test >= 0.9\r\n"
			+ "AND (model_title = '3Cattle1FarmRF_Lying' OR model_title = '3Cattle1FarmRF_Grassing' OR model_title = '3Cattle1FarmRF_Ruminating');";
	
	public static String SQL_QUERY_TRAINEDMODELS = 
			  "SELECT id, model_name, developer, binary_model, labels, sensor_system, sensor_list, algorithm, hyperparameters, frequency, window_type, window_size, window_stride, features, train_dataset, validation_method, train_valid_accuracy, test_accuracy, test_dataset, model_repository, model_size_in_bytes, created_time\r\n"
			+ "FROM public.%s\r\n"
			+ "WHERE window_stride = 100\r\n"
			+ "AND (%s);";
	
	public static List<Model> importFromExperimentResultSchema()
	{
		Properties dbProperties = new Properties();
		dbProperties.setProperty("user",user);
		dbProperties.setProperty("password",password);
		
		List<Model> models = new ArrayList<>();
		
		try
		{
			Connection connection = DriverManager.getConnection(url, dbProperties);
			connection.setAutoCommit(true);
			
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL_QUERY_EXPERIMENTRESULT, table));
			//TODO: ___ preparedStatement.setInt(1, foovalue);
			
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
				
				model.setWaiteach(1000.0d/model.getResampled_rate_in_hz());
				
				List<String> schema = new ArrayList<>();
				List<String> preprocessing = new ArrayList<>();
				List<Feature> features = new ArrayList<>();
				
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
					if(Main.properties.getProperty("schema." + currentKey) == null)
					{
						System.err.println("The schema property 'schema." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					List<String> schemaElements = Arrays.asList(Main.properties.getProperty("schema." + currentKey).split(","));
					
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
					if(Main.properties.getProperty("preprocessing." + currentKey) == null)
					{
						System.err.println("The schema property 'preprocessing." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					String preprocessingMapping = Main.properties.getProperty("preprocessing." + currentKey).toLowerCase();
					
					if(!preprocessing.contains(preprocessingMapping))
					{
						preprocessing.add(preprocessingMapping);
					}
					
					//Features
					String currentSubValue = null;
					
					for(int index = 0; index < currentValue.size(); index++)
					{
						currentSubValue = currentValue.get(index).getAsString().toLowerCase();
						
						if(Main.properties.getProperty("feature." + currentSubValue) == null)
						{
							System.err.println("The schema property 'feature." + currentSubValue + "' could not be found.");
							
							System.exit(0);
						}
						
						Feature feature = new Feature();
						
						feature.name = preprocessingMapping + "_" + Main.properties.getProperty("feature." + currentSubValue);
						feature.type = "DOUBLE";
						feature.order = 1;
						
						features.add(feature);
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
	
	public static List<Model> importFromTrainedModelsSchema(List<Integer> ids)
	{
		Properties dbProperties = new Properties();
		dbProperties.setProperty("user",user);
		dbProperties.setProperty("password",password);
		
		List<Model> models = new ArrayList<>();
		
		String idExpression = "";
		
		for(int index = 0; index < ids.size(); index++)
		{
			idExpression += "id = " + ids.get(index);
			
			if(index != ids.size() - 1)
			{
				idExpression += " OR ";
			}
		}
		
		try
		{
			Connection connection = DriverManager.getConnection(url, dbProperties);
			connection.setAutoCommit(true);
			
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL_QUERY_TRAINEDMODELS, table, idExpression));
			//TODO: ___ preparedStatement.setInt(1, foovalue);
			
			preparedStatement.setFetchSize(0);
			
			ResultSet resultSet = preparedStatement.executeQuery();
			
			Model model = null;
			
			while (resultSet.next())
			{
				model = new Model();
				
				model.setModel_title(Integer.toString(resultSet.getInt(1)));
				model.setFeatures_json_content(resultSet.getString(14));
				model.setList_of_predicted_classes(resultSet.getString(5));
				model.setResampled_rate_in_hz(resultSet.getInt(10));
				model.setAlgorithm(resultSet.getString(8));
				model.setList_of_functions(null);
				model.setList_of_axes(null);
				model.setWindow_size(resultSet.getInt(12));
				model.setWindow_stride(resultSet.getString(13));
				model.setAccuracy_test(resultSet.getDouble(18));
				model.setF1_test(null);
				
				model.setWaiteach(1000.0d/model.getResampled_rate_in_hz());
				
				model.setSize(resultSet.getInt(21));
				
				//TODO: ___
				List<String> schema = new ArrayList<>();
				List<String> preprocessing = new ArrayList<>();
				List<Feature> features = new ArrayList<>();
				
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
					if(Main.properties.getProperty("schema." + currentKey) == null)
					{
						System.err.println("The schema property 'schema." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					List<String> schemaElements = Arrays.asList(Main.properties.getProperty("schema." + currentKey).split(","));
					
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
					if(Main.properties.getProperty("preprocessing." + currentKey) == null)
					{
						System.err.println("The schema property 'preprocessing." + currentKey + "' could not be found.");
						
						System.exit(0);
					}
					
					String preprocessingMapping = Main.properties.getProperty("preprocessing." + currentKey).toLowerCase();
					
					if(!preprocessing.contains(preprocessingMapping))
					{
						preprocessing.add(preprocessingMapping);
					}
					
					//Features
					//TODO: ___ proper sorting of features
					String currentSubValue = null;
					
					for(int index = 0; index < currentValue.size(); index++)
					{
						currentSubValue = currentValue.get(index).getAsString().toLowerCase();
						
						if(Main.properties.getProperty("feature." + currentSubValue) == null)
						{
							System.err.println("The schema property 'feature." + currentSubValue + "' could not be found.");
							
							System.exit(0);
						}
						
						features.add(preprocessingMapping + "_" + Main.properties.getProperty("feature." + currentSubValue));
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