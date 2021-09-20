package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.List;

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
		List<Feature> features = new ArrayList<>();
		
		this.setInputAndAddTo(features);
		
		return features;
	}
	
	private void setInputAndAddTo(List<Feature> features)
	{
		setInputAndAddToFeatureList(ax, "ax", features);
		setInputAndAddToFeatureList(ay, "ay", features);
		setInputAndAddToFeatureList(az, "az", features);
		setInputAndAddToFeatureList(gx, "gx", features);
		setInputAndAddToFeatureList(gy, "gy", features);
		setInputAndAddToFeatureList(gz, "gz", features);
		setInputAndAddToFeatureList(accMag, "accMag", features);
		setInputAndAddToFeatureList(gyrMag, "gyrMag", features);
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
}