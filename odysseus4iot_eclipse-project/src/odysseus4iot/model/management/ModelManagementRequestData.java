package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael Sünkel
 */
public class ModelManagementRequestData
{
	public String sensor_system = null;
	public List<ModelManagementLabel> labels = null;
	public Integer maxsizeofbignode = null;
	public Integer maxsizeof_ALL = null;
	public Integer modelset_limit = null;
	
	public void addLabel(String label, Double acc_treh)
	{
		if(this.labels == null)
		{
			this.labels = new ArrayList<>();
		}
		
		ModelManagementLabel modelManagementLabel = new ModelManagementLabel();
		modelManagementLabel.label = label;
		modelManagementLabel.acc_treh = acc_treh;
		
		this.labels.add(modelManagementLabel);
	}
}