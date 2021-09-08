package percom.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import percom.graph.operator.AggregateOperator;
import percom.graph.operator.ChangedetectOperator;
import percom.graph.operator.ClassificationOperator;
import percom.graph.operator.DatabasesourceOperator;
import percom.graph.operator.MapOperator;
import percom.graph.operator.OutlierRemovingOperator;
import percom.graph.operator.ProjectOperator;
import percom.graph.operator.TimewindowOperator;
import percom.graph.operator.meta.Column;
import percom.graph.operator.meta.Schema;

//TODO: Warum kann integer timestamp nicht als starttimestamp verwendet werden? Muss timestamp in String vorliegen?
public class OperatorGenerator
{
	public DatabasesourceOperator generateDatabasesourceOperator()
	{
		DatabasesourceOperator databasesourceOperator = new DatabasesourceOperator();
		
		databasesourceOperator.table = "sensor_data_19";
		databasesourceOperator.jdbc = "jdbc:postgresql://localhost:5432/CattleDB";
		databasesourceOperator.user = "postgres";
		databasesourceOperator.password = "postgres";
		databasesourceOperator.waiteach = 100;
		
		Schema attributes = new Schema();
		attributes.addColumn(new Column("cattle_id", Integer.class));
		attributes.addColumn(new Column("timestamp", Long.class));
		attributes.addColumn(new Column("ax", Double.class));
		attributes.addColumn(new Column("ay", Double.class));
		attributes.addColumn(new Column("az", Double.class));
		attributes.addColumn(new Column("ox", Double.class));
		attributes.addColumn(new Column("oy", Double.class));
		attributes.addColumn(new Column("oz", Double.class));
		
		databasesourceOperator.attributes = attributes;
		
		databasesourceOperator.outputSchema = databasesourceOperator.attributes;
		databasesourceOperator.outputRate = 10.0d;
		databasesourceOperator.outputName = "sensorData";
		
		return databasesourceOperator;
	}
	
	public MapOperator generateMapOperator()
	{
		MapOperator mapOperator = new MapOperator();
		
		List<String> expressions = new ArrayList<>();
		expressions.add("'cattle_id'");
		expressions.add("'timestamp'");
		expressions.add("['sqrt((ax*ax)+(ay*ay)+(az*az))','amag']");
		expressions.add("['sqrt((ox*ox)+(oy*oy)+(oz*oz))','omag']");
		
		mapOperator.expressions = expressions;
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("timestamp", Long.class));
		inputSchema.addColumn(new Column("ax", Double.class));
		inputSchema.addColumn(new Column("ay", Double.class));
		inputSchema.addColumn(new Column("az", Double.class));
		inputSchema.addColumn(new Column("ox", Double.class));
		inputSchema.addColumn(new Column("oy", Double.class));
		inputSchema.addColumn(new Column("oz", Double.class));
		
		mapOperator.inputSchema = inputSchema;
		mapOperator.inputRate = 10.0d;
		mapOperator.inputName = "sensorData";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("timestamp", Long.class));
		outputSchema.addColumn(new Column("amag", Double.class));
		outputSchema.addColumn(new Column("omag", Double.class));
		
		mapOperator.outputSchema = outputSchema;
		mapOperator.outputRate = 10.0d;
		mapOperator.outputName = "sensorDataMag";
		
		return mapOperator;
	}
	
	public TimewindowOperator generateTimewindowOperator()
	{
		TimewindowOperator timewindowOperator = new TimewindowOperator();
		
		timewindowOperator.size = 5000;
		timewindowOperator.slide = 5000;
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("timestamp", Long.class));
		inputSchema.addColumn(new Column("amag", Double.class));
		inputSchema.addColumn(new Column("omag", Double.class));
		
		timewindowOperator.inputSchema = inputSchema;
		timewindowOperator.inputRate = 10.0d;
		timewindowOperator.inputName = "sensorDataMag";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("timestamp", Long.class));
		outputSchema.addColumn(new Column("amag", Double.class));
		outputSchema.addColumn(new Column("omag", Double.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		timewindowOperator.outputSchema = outputSchema;
		timewindowOperator.outputRate = 10.0d;
		timewindowOperator.outputName = "sensorDataMagWindowed";
		
		return timewindowOperator;
	}
	
	public AggregateOperator generateAggregateOperator()
	{
		AggregateOperator aggregateOperator = new AggregateOperator();
		
		aggregateOperator.group_by = "cattle_id";
		
		List<String> aggregations = new ArrayList<>();
		
		aggregations.add("['count', ['amag'], 'amag_count', 'Integer']");
		aggregations.add("['min', ['amag'], 'amag_min', 'Double']");
		aggregations.add("['min', ['omag'], 'omag_min', 'Double']");
		aggregations.add("['max', ['amag'], 'amag_max', 'Integer']");
		aggregations.add("['max', ['omag'], 'omag_max', 'Double']");
		aggregations.add("['avg', ['amag'], 'amag_avg', 'Double']");
		aggregations.add("['avg', ['omag'], 'omag_avg', 'Double']");
		aggregations.add("['median', ['amag'], 'amag_median', 'Double']");
		aggregations.add("['median', ['omag'], 'omag_median', 'Double']");
		aggregations.add("['stddev', ['amag'], 'amag_stddev', 'Double']");
		aggregations.add("['stddev', ['omag'], 'omag_stddev', 'Double']");
		aggregations.add("['kurtosis', ['amag'], 'amag_kurtosis', 'Double']");
		aggregations.add("['kurtosis', ['omag'], 'omag_kurtosis', 'Double']");
		aggregations.add("['skewness', ['amag'], 'amag_skewness', 'Double']");
		aggregations.add("['skewness', ['omag'], 'omag_skewness', 'Double']");
		aggregations.add("['iqr', ['amag'], 'amag_iqr', 'Double']");
		aggregations.add("['iqr', ['omag'], 'omag_iqr', 'Double']");
		aggregations.add("['mcr', ['amag'], 'amag_mcr', 'Double']");
		aggregations.add("['mcr', ['omag'], 'omag_mcr', 'Double']");
		aggregations.add("['rms', ['amag'], 'amag_rms', 'Double']");
		aggregations.add("['rms', ['omag'], 'omag_rms', 'Double']");
		aggregations.add("['FrEnergy', ['amag'], 'amag_FrEnergy', 'Double']");
		aggregations.add("['FrEnergy', ['omag'], 'omag_FrEnergy', 'Double']");
		aggregations.add("['FrDmEntropy', ['amag'], 'amag_FrDmEntroPy', 'Double']");
		aggregations.add("['FrDmEntropy', ['omag'], 'omag_FrDmEntroPy', 'Double']");
		aggregations.add("['FrPeakFreq', ['amag'], 'amag_FrPeakFreq', 'Double']");
		aggregations.add("['FrPeakFreq', ['omag'], 'omag_FrPeakFreq', 'Double']");
		aggregations.add("['FrMag1', ['amag'], 'amag_FrMag1', 'Double']");
		aggregations.add("['FrMag2', ['amag'], 'amag_FrMag2', 'Double']");
		aggregations.add("['FrMag3', ['amag'], 'amag_FrMag3', 'Double']");
		aggregations.add("['FrMag4', ['amag'], 'amag_FrMag4', 'Double']");
		aggregations.add("['FrMag5', ['amag'], 'amag_FrMag5', 'Double']");
		aggregations.add("['FrMag1', ['omag'], 'omag_FrMag1', 'Double']");
		aggregations.add("['FrMag2', ['omag'], 'omag_FrMag2', 'Double']");
		aggregations.add("['FrMag3', ['omag'], 'omag_FrMag3', 'Double']");
		aggregations.add("['FrMag4', ['omag'], 'omag_FrMag4', 'Double']");
		aggregations.add("['FrMag5', ['omag'], 'omag_FrMag5', 'Double']");
		
		aggregateOperator.aggregations = aggregations;
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("timestamp", Long.class));
		inputSchema.addColumn(new Column("amag", Double.class));
		inputSchema.addColumn(new Column("omag", Double.class));
		inputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		aggregateOperator.inputSchema = inputSchema;
		aggregateOperator.inputRate = 10.0d;
		aggregateOperator.inputName = "sensorDataMagWindowed";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("car_amag_min", Double.class));
		outputSchema.addColumn(new Column("car_omag_min", Double.class));
		outputSchema.addColumn(new Column("car_amag_max", Double.class));
		outputSchema.addColumn(new Column("car_omag_max", Double.class));
		outputSchema.addColumn(new Column("car_amag_avg", Double.class));
		outputSchema.addColumn(new Column("car_omag_avg", Double.class));
		outputSchema.addColumn(new Column("car_amag_median", Double.class));
		outputSchema.addColumn(new Column("car_omag_median", Double.class));
		outputSchema.addColumn(new Column("car_amag_stddev", Double.class));
		outputSchema.addColumn(new Column("car_omag_stddev", Double.class));
		outputSchema.addColumn(new Column("car_amag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("car_omag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("car_amag_skewness", Double.class));
		outputSchema.addColumn(new Column("car_omag_skewness", Double.class));
		outputSchema.addColumn(new Column("car_amag_iqr", Double.class));
		outputSchema.addColumn(new Column("car_omag_iqr", Double.class));
		outputSchema.addColumn(new Column("car_amag_mcr", Double.class));
		outputSchema.addColumn(new Column("car_omag_mcr", Double.class));
		outputSchema.addColumn(new Column("car_amag_rms", Double.class));
		outputSchema.addColumn(new Column("car_omag_rms", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag5", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag5", Double.class));
		outputSchema.addColumn(new Column("start_timestamp", Long.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		aggregateOperator.outputSchema = outputSchema;
		aggregateOperator.outputRate = 0.2d;
		aggregateOperator.outputName = "features";
		
		return aggregateOperator;
	}
	
	public ProjectOperator generateProjectOperator()
	{
		ProjectOperator projectOperator = new ProjectOperator();
		
		List<String> attributes = new ArrayList<>();
		attributes.add("'cattle_id'");
		attributes.add("'car_amag_min'");
		attributes.add("'car_omag_min'");
		attributes.add("'car_amag_max'");
		attributes.add("'car_omag_max'");
		attributes.add("'car_amag_avg'");
		attributes.add("'car_omag_avg'");
		attributes.add("'car_amag_median'");
		attributes.add("'car_omag_median'");
		attributes.add("'car_amag_stddev'");
		attributes.add("'car_omag_stddev'");
		attributes.add("'car_amag_kurtosis'");
		attributes.add("'car_omag_kurtosis'");
		attributes.add("'car_amag_skewness'");
		attributes.add("'car_omag_skewness'");
		attributes.add("'car_amag_iqr'");
		attributes.add("'car_omag_iqr'");
		attributes.add("'car_amag_mcr'");
		attributes.add("'car_omag_mcr'");
		attributes.add("'car_amag_rms'");
		attributes.add("'car_omag_rms'");
		attributes.add("'car_amag_FrEnergy'");
		attributes.add("'car_omag_FrEnergy'");
		attributes.add("'car_amag_FrDmEntroPy'");
		attributes.add("'car_omag_FrDmEntroPy'");
		attributes.add("'car_amag_FrPeakFreq'");
		attributes.add("'car_omag_FrPeakFreq'");
		attributes.add("'car_amag_FrMag1'");
		attributes.add("'car_amag_FrMag2'");
		attributes.add("'car_amag_FrMag3'");
		attributes.add("'car_amag_FrMag4'");
		attributes.add("'car_amag_FrMag5'");
		attributes.add("'car_omag_FrMag1'");
		attributes.add("'car_omag_FrMag2'");
		attributes.add("'car_omag_FrMag3'");
		attributes.add("'car_omag_FrMag4'");
		attributes.add("'car_omag_FrMag5'");
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("car_amag_min", Double.class));
		inputSchema.addColumn(new Column("car_omag_min", Double.class));
		inputSchema.addColumn(new Column("car_amag_max", Double.class));
		inputSchema.addColumn(new Column("car_omag_max", Double.class));
		inputSchema.addColumn(new Column("car_amag_avg", Double.class));
		inputSchema.addColumn(new Column("car_omag_avg", Double.class));
		inputSchema.addColumn(new Column("car_amag_median", Double.class));
		inputSchema.addColumn(new Column("car_omag_median", Double.class));
		inputSchema.addColumn(new Column("car_amag_stddev", Double.class));
		inputSchema.addColumn(new Column("car_omag_stddev", Double.class));
		inputSchema.addColumn(new Column("car_amag_kurtosis", Double.class));
		inputSchema.addColumn(new Column("car_omag_kurtosis", Double.class));
		inputSchema.addColumn(new Column("car_amag_skewness", Double.class));
		inputSchema.addColumn(new Column("car_omag_skewness", Double.class));
		inputSchema.addColumn(new Column("car_amag_iqr", Double.class));
		inputSchema.addColumn(new Column("car_omag_iqr", Double.class));
		inputSchema.addColumn(new Column("car_amag_mcr", Double.class));
		inputSchema.addColumn(new Column("car_omag_mcr", Double.class));
		inputSchema.addColumn(new Column("car_amag_rms", Double.class));
		inputSchema.addColumn(new Column("car_omag_rms", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrEnergy", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrEnergy", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrDmEntroPy", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrDmEntroPy", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrPeakFreq", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrPeakFreq", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag1", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag2", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag3", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag4", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag5", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag1", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag2", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag3", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag4", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag5", Double.class));
		inputSchema.addColumn(new Column("start_timestamp", Long.class));
		inputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		projectOperator.inputSchema = inputSchema;
		projectOperator.inputRate = 0.2d;
		projectOperator.inputName = "features";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("car_amag_min", Double.class));
		outputSchema.addColumn(new Column("car_omag_min", Double.class));
		outputSchema.addColumn(new Column("car_amag_max", Double.class));
		outputSchema.addColumn(new Column("car_omag_max", Double.class));
		outputSchema.addColumn(new Column("car_amag_avg", Double.class));
		outputSchema.addColumn(new Column("car_omag_avg", Double.class));
		outputSchema.addColumn(new Column("car_amag_median", Double.class));
		outputSchema.addColumn(new Column("car_omag_median", Double.class));
		outputSchema.addColumn(new Column("car_amag_stddev", Double.class));
		outputSchema.addColumn(new Column("car_omag_stddev", Double.class));
		outputSchema.addColumn(new Column("car_amag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("car_omag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("car_amag_skewness", Double.class));
		outputSchema.addColumn(new Column("car_omag_skewness", Double.class));
		outputSchema.addColumn(new Column("car_amag_iqr", Double.class));
		outputSchema.addColumn(new Column("car_omag_iqr", Double.class));
		outputSchema.addColumn(new Column("car_amag_mcr", Double.class));
		outputSchema.addColumn(new Column("car_omag_mcr", Double.class));
		outputSchema.addColumn(new Column("car_amag_rms", Double.class));
		outputSchema.addColumn(new Column("car_omag_rms", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("car_amag_FrMag5", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("car_omag_FrMag5", Double.class));
		outputSchema.addColumn(new Column("start_timestamp", Long.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		projectOperator.outputSchema = outputSchema;
		projectOperator.outputRate = 0.2d;
		projectOperator.outputName = "features_projected";
		
		return projectOperator;
	}
	
	public ClassificationOperator generateClassificationOperator()
	{
		ClassificationOperator classificationOperator = new ClassificationOperator();
		
		classificationOperator.database = "CattleDB";
		classificationOperator.host = "localhost";
		classificationOperator.port = 5432;
		classificationOperator.rpcServer = "localhost:9000";
		classificationOperator.table = "experiment_result";
		classificationOperator.username = "postgres";
		classificationOperator.password = "postgres";
		classificationOperator.selectmodelbycolumn = "model_title";
		classificationOperator.selectmodelbyvalue = "3Cattle1FarmRF_Lying";
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("car_amag_min", Double.class));
		inputSchema.addColumn(new Column("car_omag_min", Double.class));
		inputSchema.addColumn(new Column("car_amag_max", Double.class));
		inputSchema.addColumn(new Column("car_omag_max", Double.class));
		inputSchema.addColumn(new Column("car_amag_avg", Double.class));
		inputSchema.addColumn(new Column("car_omag_avg", Double.class));
		inputSchema.addColumn(new Column("car_amag_median", Double.class));
		inputSchema.addColumn(new Column("car_omag_median", Double.class));
		inputSchema.addColumn(new Column("car_amag_stddev", Double.class));
		inputSchema.addColumn(new Column("car_omag_stddev", Double.class));
		inputSchema.addColumn(new Column("car_amag_kurtosis", Double.class));
		inputSchema.addColumn(new Column("car_omag_kurtosis", Double.class));
		inputSchema.addColumn(new Column("car_amag_skewness", Double.class));
		inputSchema.addColumn(new Column("car_omag_skewness", Double.class));
		inputSchema.addColumn(new Column("car_amag_iqr", Double.class));
		inputSchema.addColumn(new Column("car_omag_iqr", Double.class));
		inputSchema.addColumn(new Column("car_amag_mcr", Double.class));
		inputSchema.addColumn(new Column("car_omag_mcr", Double.class));
		inputSchema.addColumn(new Column("car_amag_rms", Double.class));
		inputSchema.addColumn(new Column("car_omag_rms", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrEnergy", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrEnergy", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrDmEntroPy", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrDmEntroPy", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrPeakFreq", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrPeakFreq", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag1", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag2", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag3", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag4", Double.class));
		inputSchema.addColumn(new Column("car_amag_FrMag5", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag1", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag2", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag3", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag4", Double.class));
		inputSchema.addColumn(new Column("car_omag_FrMag5", Double.class));
		inputSchema.addColumn(new Column("start_timestamp", Long.class));
		inputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		classificationOperator.inputSchema = inputSchema;
		classificationOperator.inputRate = 0.2d;
		classificationOperator.inputName = "features";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		outputSchema.addColumn(new Column("start_timestamp", Long.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		classificationOperator.outputSchema = outputSchema;
		classificationOperator.outputRate = 0.2d;
		classificationOperator.outputName = "classification";
		
		return classificationOperator;
	}
	
	public OutlierRemovingOperator generateOutlierRemovingOperator()
	{
		OutlierRemovingOperator outlierRemovingOperator = new OutlierRemovingOperator();
		
		outlierRemovingOperator.group_by = "cattle_id";
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("prediction", String.class));
		inputSchema.addColumn(new Column("start_timestamp", Long.class));
		inputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		outlierRemovingOperator.inputSchema = inputSchema;
		outlierRemovingOperator.inputRate = 0.2d;
		outlierRemovingOperator.inputName = "classification";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		outputSchema.addColumn(new Column("start_timestamp", Long.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		outlierRemovingOperator.outputSchema = outputSchema;
		outlierRemovingOperator.outputRate = 0.2d;
		outlierRemovingOperator.outputName = "classification_cleaned";
		
		return outlierRemovingOperator;
	}
	
	public ChangedetectOperator generateChangedetectOperator()
	{
		ChangedetectOperator changedetectOperator = new ChangedetectOperator();
		
		changedetectOperator.attr = "prediction";
		changedetectOperator.group_by = "cattle_id";
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("prediction", String.class));
		inputSchema.addColumn(new Column("start_timestamp", Long.class));
		inputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		changedetectOperator.inputSchema = inputSchema;
		changedetectOperator.inputRate = 0.2d;
		changedetectOperator.inputName = "classification_cleaned";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		outputSchema.addColumn(new Column("start_timestamp", Long.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		changedetectOperator.outputSchema = outputSchema;
		changedetectOperator.outputRate = null;
		changedetectOperator.outputName = "output";
		
		return changedetectOperator;
	}
}