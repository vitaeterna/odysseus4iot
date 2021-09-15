package odysseus4iot.model;

import java.util.ArrayList;
import java.util.List;

public class Model
{
	private String model_title = null;
	private String features_json_content = null;
	private String list_of_predicted_classes = null;
	private Integer resampled_rate_in_hz = null;
	private String algorithm = null;
	private String list_of_functions = null;
	private String list_of_axes = null;
	private Integer window_size = null;
	private String window_stride = null;
	private Double accuracy_test = null;
	private Double f1_test = null;
	
	private Integer window_slide = null;
	private Double waiteach = null;
	private List<String> schema = null;
	private List<String> preprocessing = null;
	private List<String> features = null;
	
	public String getModel_title() {
		return model_title;
	}
	public void setModel_title(String model_title) {
		this.model_title = model_title;
	}
	public String getFeatures_json_content() {
		return features_json_content;
	}
	public void setFeatures_json_content(String features_json_content) {
		this.features_json_content = features_json_content;
	}
	public String getList_of_predicted_classes() {
		return list_of_predicted_classes;
	}
	public void setList_of_predicted_classes(String list_of_predicted_classes) {
		this.list_of_predicted_classes = list_of_predicted_classes;
	}
	public Integer getResampled_rate_in_hz() {
		return resampled_rate_in_hz;
	}
	public void setResampled_rate_in_hz(Integer resampled_rate_in_hz) {
		this.resampled_rate_in_hz = resampled_rate_in_hz;
	}
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public String getList_of_functions() {
		return list_of_functions;
	}
	public void setList_of_functions(String list_of_functions) {
		this.list_of_functions = list_of_functions;
	}
	public String getList_of_axes() {
		return list_of_axes;
	}
	public void setList_of_axes(String list_of_axes) {
		this.list_of_axes = list_of_axes;
	}
	public Integer getWindow_size() {
		return window_size;
	}
	public void setWindow_size(Integer window_size) {
		this.window_size = window_size;
		if(this.window_size != null && this.window_stride != null)
		{
			this.window_slide = (int) (this.window_size * (Double.parseDouble(window_stride.replace("%", "")) / 100.0d));
		}
	}
	public String getWindow_stride() {
		return window_stride;
	}
	public void setWindow_stride(String window_stride) {
		this.window_stride = window_stride;
		if(this.window_size != null && this.window_stride != null)
		{
			this.window_slide = (int) (this.window_size * (Double.parseDouble(window_stride.replace("%", "")) / 100.0d));
		}
	}
	public Double getAccuracy_test() {
		return accuracy_test;
	}
	public void setAccuracy_test(Double accuracy_test) {
		this.accuracy_test = accuracy_test;
	}
	public Double getF1_test() {
		return f1_test;
	}
	public void setF1_test(Double f1_test) {
		this.f1_test = f1_test;
	}
	public Integer getWindow_slide() {
		return window_slide;
	}
	public Double getWaiteach() {
		return waiteach;
	}
	public void setWaiteach(Double waiteach) {
		this.waiteach = waiteach;
	}
	public List<String> getSchema() {
		return schema;
	}
	public void setSchema(List<String> schema) {
		this.schema = schema;
	}
	public List<String> getPreprocessing() {
		return preprocessing;
	}
	public void setPreprocessing(List<String> preprocessing) {
		this.preprocessing = preprocessing;
	}
	public List<String> getFeatures() {
		return features;
	}
	public void setFeatures(List<String> features) {
		this.features = features;
	}
	
	@Override
	public String toString()
	{
		StringBuilder stringBuilder = new StringBuilder();
		
		stringBuilder.append("model_title:              " + model_title + "\r\n");
		stringBuilder.append("features_json_content:    " + features_json_content + "\r\n");
		stringBuilder.append("list_of_predicted_classes:" + list_of_predicted_classes + "\r\n");
		stringBuilder.append("resampled_rate_in_hz:     " + resampled_rate_in_hz + "\r\n");
		stringBuilder.append("algorithm:                " + algorithm + "\r\n");
		stringBuilder.append("list_of_functions:        " + list_of_functions + "\r\n");
		stringBuilder.append("list_of_axes:             " + list_of_axes + "\r\n");
		stringBuilder.append("window_size:              " + window_size + "\r\n");
		stringBuilder.append("window_stride:            " + window_stride + " (window_slide:" + window_slide + ")\r\n");
		stringBuilder.append("accuracy_test:            " + accuracy_test + "\r\n");
		stringBuilder.append("f1_test:                  " + f1_test + "\r\n");
		stringBuilder.append("schema:                   " + schema + "\r\n");
		stringBuilder.append("preprocessing:            " + preprocessing + "\r\n");
		stringBuilder.append("features:                 " + features);
		
		return stringBuilder.toString();
	}
	
	public static List<String> getUnionOfSchemata(List<Model> models)
	{
		List<String> unionOfSchemata = new ArrayList<>();
		
		List<String> currentSchema = null;
		
		String currentName = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentSchema = models.get(index).schema;
			
			for(int index2 = 0; index2 < currentSchema.size(); index2++)
			{
				currentName = currentSchema.get(index2);
				
				if(!unionOfSchemata.contains(currentName))
				{
					unionOfSchemata.add(currentName);
				}
			}
		}
		
		return unionOfSchemata;
	}
	
	public static Double getMinWaiteach(List<Model> models)
	{
		Double minWaiteach = Double.MAX_VALUE;
		
		for(int index = 0; index < models.size(); index++)
		{
			if(minWaiteach > models.get(index).waiteach)
			{
				minWaiteach = models.get(index).waiteach;
			}
		}
		
		return minWaiteach;
	}
	
	public static List<String> getUnionOfPreprocessing(List<Model> models)
	{
		List<String> unionOfPreprocessing = new ArrayList<>();
		
		List<String> currentPreprocessing = null;
		
		String currentName = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentPreprocessing = models.get(index).preprocessing;
			
			for(int index2 = 0; index2 < currentPreprocessing.size(); index2++)
			{
				currentName = currentPreprocessing.get(index2);
				
				if(!unionOfPreprocessing.contains(currentName))
				{
					unionOfPreprocessing.add(currentName);
				}
			}
		}
		
		return unionOfPreprocessing;
	}
	
	public static List<String> getUnionOfFeatures(List<Model> models)
	{
		List<String> unionOfFeatures = new ArrayList<>();
		
		List<String> currentFeatures = null;
		
		String currentName = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentFeatures = models.get(index).features;
			
			for(int index2 = 0; index2 < currentFeatures.size(); index2++)
			{
				currentName = currentFeatures.get(index2);
				
				if(!unionOfFeatures.contains(currentName))
				{
					unionOfFeatures.add(currentName);
				}
			}
		}
		
		return unionOfFeatures;
	}
}