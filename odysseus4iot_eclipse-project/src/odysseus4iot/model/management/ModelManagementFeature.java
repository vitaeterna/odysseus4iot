package odysseus4iot.model.management;

import odysseus4iot.main.Main;
import odysseus4iot.model.Feature;

public class ModelManagementFeature implements Comparable<ModelManagementFeature>
{
	public String function = null;
	public String type = null;
	public Integer order = null;
	
	public String input = null;
	
	public Feature toFeature()
	{
		Feature feature = new Feature();
		
		if(Main.properties.getProperty("preprocessing." + this.input.toLowerCase()) == null)
		{
			System.err.println("The schema property 'preprocessing." + this.input.toLowerCase() + "' could not be found.");
			
			System.exit(0);
		}
		
		if(Main.properties.getProperty("feature." + this.function.toLowerCase()) == null)
		{
			System.err.println("The schema property 'feature." + this.function.toLowerCase() + "' could not be found.");
			
			System.exit(0);
		}

		feature.name = (Main.properties.getProperty("preprocessing." + this.input.toLowerCase()) + "_" + Main.properties.getProperty("feature." + this.function.toLowerCase())).toLowerCase();
		feature.type = this.type.toUpperCase();
		feature.order = this.order;
		
		return feature;
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
			ModelManagementFeature modelManagementFeature = (ModelManagementFeature)object;
			
			if((modelManagementFeature.input + "_" + modelManagementFeature.function).equals(this.input + "_" + this.function))
			{
				if(modelManagementFeature.type.equals(this.type))
				{
					return true;
				}
			}
		}
		
		return false;
	}
	
	@Override
	public int compareTo(ModelManagementFeature modelManagementFeature)
	{
		if(this.order.intValue() != modelManagementFeature.order.intValue())
		{
			return Integer.compare(this.order, modelManagementFeature.order);
		}
		else
		{
			return (this.input + "_" + this.function).compareTo(modelManagementFeature.input + "_" + modelManagementFeature.function);
		}
	}
}