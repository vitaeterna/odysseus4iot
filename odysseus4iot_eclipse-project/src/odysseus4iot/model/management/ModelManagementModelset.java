package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael Sünkel
 */
public class ModelManagementModelset
{
	public String name = null;
	public Double avg_accuracy = null;
	public Integer query_sharing_level = null;
	public Double final_score = null;
	public Integer number_of_models = null;
	public List<ModelManagementModel> ml_models = null;
	
	public List<Integer> getModelIDs()
	{
		List<Integer> modelIDs = new ArrayList<>();
		
		for(int index = 0; index < ml_models.size(); index++)
		{
			modelIDs.add(ml_models.get(index).model_id);
		}
		
		return modelIDs;
	}
}