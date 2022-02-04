package odysseus4iot.model.management;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.model.Feature;
import odysseus4iot.model.Model;

/**
 * @author Michael Sünkel
 */
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
	public Long size = null;
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
		
		this.features.processFeatures(schema, preprocessing, features);
		
		model.setSchema(schema);
		model.setPreprocessing(preprocessing);
		model.setFeatures(features);
		
		return model;
	}
}