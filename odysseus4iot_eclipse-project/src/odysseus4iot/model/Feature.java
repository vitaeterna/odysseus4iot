package odysseus4iot.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael Sünkel
 */
public class Feature implements Comparable<Feature>
{
	public String name = null;
	public String type = null;
	public Integer order = null;
	
	@Override
	public String toString()
	{
		return name;
	}

	@Override
	public boolean equals(Object object)
	{
		if(object == null)
		{
			return false;
		}
		
		if(object == this)
		{
			return true;
		}
		
		if(object.getClass() == this.getClass())
		{
			Feature feature = (Feature)object;
			
			if(feature.name.equals(this.name) && feature.type.equals(this.type))
			{
				return true;
			}
		}
		
		return false;
	}

	@Override
	public int compareTo(Feature Feature)
	{
		if(this.order.intValue() != Feature.order.intValue())
		{
			return Integer.compare(this.order, Feature.order);
		}
		else
		{
			return this.name.compareTo(Feature.name);
		}
	}
	
	public static List<String> getNamesFromFeatures(List<Feature> features)
	{
		List<String> names = new ArrayList<>();
		
		for(int index = 0; index < features.size(); index++)
		{
			names.add(features.get(index).name);
		}
		
		return names;
	}
	
	public static List<Feature> getFeaturesFromNames(List<String> names)
	{
		List<Feature> features = new ArrayList<>();
		
		Feature feature = null;
		
		for(int index = 0; index < names.size(); index++)
		{
			feature = new Feature();
			
			feature.name = names.get(index);
			feature.type = "DOUBLE";
			feature.order = 1;
		}
		
		return features;
	}
}