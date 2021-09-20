package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import odysseus4iot.main.Main;
import odysseus4iot.model.Feature;
import odysseus4iot.model.Model;

public class ModelManagementModel
{
	public Integer model_id = null;
	public Boolean type = null;
	public List<String> labels = null;
	public Double accuracy = null;
	public Integer sensor_system = null;
	public String window_type = null;
	public String window_size = null;
	public String window_stride = null;
	public ModelManagementFeatures features = null;
	public Integer size = null;
	public String algorithm = null;
	
	public Model toModel()
	{
		Model model = new Model();
		
		model.setModel_title(model_id.toString());
		model.setFeatures_json_content(null);
		model.setList_of_predicted_classes(labels.toString());
		model.setResampled_rate_in_hz(10);
		model.setAlgorithm(algorithm);
		model.setList_of_functions(null);
		model.setList_of_axes(null);
		model.setWindow_size(Integer.parseInt(window_size));
		model.setWindow_stride(window_stride);
		model.setAccuracy_test(accuracy);
		model.setF1_test(null);
		
		model.setWaiteach(1000.0d/model.getResampled_rate_in_hz());
		
		model.setSize(size);
		
		List<String> schema = new ArrayList<>();
		List<String> preprocessing = new ArrayList<>();
		List<Feature> features = new ArrayList<>();
		
		List<String> keys = new ArrayList<>();
		
		if(this.features.ax != null)
		{
			keys.add("ax");
		}
		if(this.features.ay != null)
		{
			keys.add("ay");
		}
		if(this.features.az != null)
		{
			keys.add("az");
		}
		if(this.features.gx != null)
		{
			keys.add("gx");
		}
		if(this.features.gy != null)
		{
			keys.add("gy");
		}
		if(this.features.gz != null)
		{
			keys.add("gz");
		}
		if(this.features.accMag != null)
		{
			keys.add("accMag");
		}
		if(this.features.gyrMag != null)
		{
			keys.add("gyrMag");
		}
		
		String currentKey = null;
		
		for(int index = 0; index < keys.size(); index++)
		{
			currentKey = keys.get(index);
			
			//Schema
			if(Main.properties.getProperty("schema." + currentKey) == null)
			{
				System.err.println("The schema property 'schema." + currentKey + "' could not be found.");
				
				System.exit(0);
			}
			
			List<String> schemaElements = Arrays.asList(Main.properties.getProperty("schema." + currentKey).split(","));
			
			String currentSchemaElement = null;
			
			for(int index2 = 0; index2 < schemaElements.size(); index2++)
			{
				currentSchemaElement = schemaElements.get(index2).toLowerCase();
				
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
		}
		
		//Features
		features = this.features.getFeatures();
		
		Collections.sort(schema);
		Collections.sort(preprocessing);
		Collections.sort(features);
		
		model.setSchema(schema);
		model.setPreprocessing(preprocessing);
		model.setFeatures(features);
		
		return model;
	}
}