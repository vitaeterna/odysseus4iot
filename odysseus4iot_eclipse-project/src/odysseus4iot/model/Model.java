package odysseus4iot.model;

import java.util.Arrays;
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
	
	private String _modelTitle = null;
	private List<String> _preprocessing = null;
	private List<String> _features = null;
	private Integer _windowSize = null;
	private Integer _windowStride = null;
	
	public String getModel_title() {
		return model_title;
	}
	public void setModel_title(String model_title) {
		this.model_title = model_title;
		this._modelTitle = model_title;
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
		this._features = Arrays.asList(list_of_functions.split("_"));
	}
	public String getList_of_axes() {
		return list_of_axes;
	}
	public void setList_of_axes(String list_of_axes) {
		this.list_of_axes = list_of_axes;
		this._preprocessing = Arrays.asList(list_of_axes.split("_"));
	}
	public Integer getWindow_size() {
		return window_size;
	}
	public void setWindow_size(Integer window_size) {
		this.window_size = window_size;
		this._windowSize = window_size;
	}
	public String getWindow_stride() {
		return window_stride;
	}
	public void setWindow_stride(String window_stride) {
		this.window_stride = window_stride;
		this._windowStride = (int) (_windowSize * (Integer.parseInt(window_stride.replace("%", "")) / 100.0d));
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
	
	public String get_modelTitle() {
		return _modelTitle;
	}
	public void set_modelTitle(String _modelTitle) {
		this._modelTitle = _modelTitle;
	}
	public List<String> get_preprocessing() {
		return _preprocessing;
	}
	public void set_preprocessing(List<String> _preprocessing) {
		this._preprocessing = _preprocessing;
	}
	public List<String> get_features() {
		return _features;
	}
	public void set_features(List<String> _features) {
		this._features = _features;
	}
	public Integer get_windowSize() {
		return _windowSize;
	}
	public void set_windowSize(Integer _windowSize) {
		this._windowSize = _windowSize;
	}
	public Integer get_windowStride() {
		return _windowStride;
	}
	public void set_windowStride(Integer _windowStride) {
		this._windowStride = _windowStride;
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
		stringBuilder.append("window_stride:            " + window_stride + "\r\n");
		stringBuilder.append("accuracy_test:            " + accuracy_test + "\r\n");
		stringBuilder.append("f1_test:                  " + f1_test);
		
		return stringBuilder.toString();
	}
}