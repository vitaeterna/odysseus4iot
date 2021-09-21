package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import odysseus4iot.main.Main;
import odysseus4iot.model.Feature;

public class ModelManagementFeatures
{
	public List<ModelManagementFeature> ax = null;
	public List<ModelManagementFeature> ay = null;
	public List<ModelManagementFeature> az = null;
	public List<ModelManagementFeature> gx = null;
	public List<ModelManagementFeature> gy = null;
	public List<ModelManagementFeature> gz = null;
	public List<ModelManagementFeature> accMag = null;
	public List<ModelManagementFeature> gyrMag = null;
	
	public List<Feature> getFeatures()
	{
		List<Feature> addToList = new ArrayList<>();
		
		this.setInputAndAddTo(addToList);
		
		return addToList;
	}
	
	private void setInputAndAddTo(List<Feature> addToList)
	{
		setInputAndAddToFeatureList(ax, "ax", addToList);
		setInputAndAddToFeatureList(ay, "ay", addToList);
		setInputAndAddToFeatureList(az, "az", addToList);
		setInputAndAddToFeatureList(gx, "gx", addToList);
		setInputAndAddToFeatureList(gy, "gy", addToList);
		setInputAndAddToFeatureList(gz, "gz", addToList);
		setInputAndAddToFeatureList(accMag, "accMag", addToList);
		setInputAndAddToFeatureList(gyrMag, "gyrMag", addToList);
	}
	
	private void setInputAndAddToFeatureList(List<ModelManagementFeature> features, String input, List<Feature> addToList)
	{
		if(features != null)
		{
			for(int index = 0; index < features.size(); index++)
			{
				features.get(index).input = input;
				
				addToList.add(features.get(index).toFeature());
			}
		}
	}
	
	public void processFeatures(List<String> schema, List<String> preprocessing, List<Feature> features)
	{
		List<String> keys = new ArrayList<>();
		
		if(this.ax != null)
		{
			keys.add("ax");
		}
		if(this.ay != null)
		{
			keys.add("ay");
		}
		if(this.az != null)
		{
			keys.add("az");
		}
		if(this.gx != null)
		{
			keys.add("gx");
		}
		if(this.gy != null)
		{
			keys.add("gy");
		}
		if(this.gz != null)
		{
			keys.add("gz");
		}
		if(this.accMag != null)
		{
			keys.add("accMag");
		}
		if(this.gyrMag != null)
		{
			keys.add("gyrMag");
		}
		
		String currentKey = null;
		
		for(int index = 0; index < keys.size(); index++)
		{
			currentKey = keys.get(index).toLowerCase();
			
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
		features.addAll(this.getFeatures());
		
		Collections.sort(schema);
		Collections.sort(preprocessing);
		Collections.sort(features);
	}
}