package odysseus4iot.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.Schema;
import odysseus4iot.main.Main;

//TODO: Warum kann integer timestamp nicht als starttimestamp verwendet werden? Muss timestamp in String vorliegen?
public class OperatorGenerator
{
	public static DatabasesourceOperator generateDatabasesourceOperator(String sensor, List<String> schema, Integer waiteach)
	{
		DatabasesourceOperator databasesourceOperator = new DatabasesourceOperator();
		
		databasesourceOperator.table = sensor;
		databasesourceOperator.jdbc = Main.properties.getProperty("sensordb.url");
		databasesourceOperator.user = Main.properties.getProperty("sensordb.user");
		databasesourceOperator.password = Main.properties.getProperty("sensordb.password");
		databasesourceOperator.waiteach = waiteach;
		
		Schema attributes = new Schema();
		attributes.addColumn(new Column("cattle_id", Integer.class));
		
		String columnName = null;
		
		for(int index = 0; index < schema.size(); index++)
		{
			columnName = schema.get(index);
			
			attributes.addColumn(new Column(columnName, Double.class));
		}
		
		databasesourceOperator.attributes = attributes;
		
		databasesourceOperator.outputSchema = databasesourceOperator.attributes;
		databasesourceOperator.outputRate = 1000.0d/((double)waiteach);
		databasesourceOperator.outputName = sensor;
		
		return databasesourceOperator;
	}
	
	public static MapOperator generateMapOperator(List<String> preprocessing)
	{
		MapOperator mapOperator = new MapOperator();
		
		List<String> expressions = new ArrayList<>();
		expressions.add("'cattle_id'");
		
		String expressionName = null;
		
		for(int index = 0; index < preprocessing.size(); index++)
		{
			expressionName = preprocessing.get(index);
			
			if(expressionName.equals("amag"))
			{
				expressions.add("['sqrt((ax*ax)+(ay*ay)+(az*az))','amag']");
			}
			else if(expressionName.equals("omag"))
			{
				expressions.add("['sqrt((ox*ox)+(oy*oy)+(oz*oz))','omag']");
			}
			else
			{
				expressions.add("'" + expressionName + "'");
			}
		}
		
		mapOperator.expressions = expressions;
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		
		for(int index = 0; index < preprocessing.size(); index++)
		{
			expressionName = preprocessing.get(index);
			
			outputSchema.addColumn(new Column(expressionName, Double.class));
		}

		mapOperator.outputSchema = outputSchema;
		mapOperator.outputRate = null;
		mapOperator.outputName = "sensor_data_map";
		
		return mapOperator;
	}
	
	public static TimewindowOperator generateTimewindowOperator(Integer size, Integer slide)
	{
		TimewindowOperator timewindowOperator = new TimewindowOperator();
		
		timewindowOperator.size = size;
		timewindowOperator.slide = slide;
		
		timewindowOperator.outputSchema = null;
		timewindowOperator.outputRate = null;
		timewindowOperator.outputName = "sensor_data_map_window";
		
		return timewindowOperator;
	}
	
	public static AggregateOperator generateAggregateOperator()
	{
		AggregateOperator aggregateOperator = new AggregateOperator();
		
		aggregateOperator.group_by = "cattle_id";
		
		List<String> aggregations = new ArrayList<>();
		
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
		inputSchema.addColumn(new Column("amag", Double.class));
		inputSchema.addColumn(new Column("omag", Double.class));
		
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
		
		aggregateOperator.outputSchema = outputSchema;
		aggregateOperator.outputRate = 0.2d;
		aggregateOperator.outputName = "features";
		
		return aggregateOperator;
	}
	
	public static ProjectOperator generateProjectOperator()
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
		
		projectOperator.outputSchema = outputSchema;
		projectOperator.outputRate = 0.2d;
		projectOperator.outputName = "features_projected";
		
		return projectOperator;
	}
	
	public static ClassificationOperator generateClassificationOperator()
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
		
		classificationOperator.inputSchema = inputSchema;
		classificationOperator.inputRate = 0.2d;
		classificationOperator.inputName = "features";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		
		classificationOperator.outputSchema = outputSchema;
		classificationOperator.outputRate = 0.2d;
		classificationOperator.outputName = "classification";
		
		return classificationOperator;
	}
	
	public static OutlierRemovingOperator generateOutlierRemovingOperator()
	{
		OutlierRemovingOperator outlierRemovingOperator = new OutlierRemovingOperator();
		
		outlierRemovingOperator.group_by = "cattle_id";
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("prediction", String.class));
		
		outlierRemovingOperator.inputSchema = inputSchema;
		outlierRemovingOperator.inputRate = 0.2d;
		outlierRemovingOperator.inputName = "classification";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		
		outlierRemovingOperator.outputSchema = outputSchema;
		outlierRemovingOperator.outputRate = 0.2d;
		outlierRemovingOperator.outputName = "classification_cleaned";
		
		return outlierRemovingOperator;
	}
	
	public static ChangedetectOperator generateChangedetectOperator()
	{
		ChangedetectOperator changedetectOperator = new ChangedetectOperator();
		
		changedetectOperator.attr = "prediction";
		changedetectOperator.group_by = "cattle_id";
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("prediction", String.class));
		
		changedetectOperator.inputSchema = inputSchema;
		changedetectOperator.inputRate = 0.2d;
		changedetectOperator.inputName = "classification_cleaned";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("prediction", String.class));
		
		changedetectOperator.outputSchema = outputSchema;
		changedetectOperator.outputRate = null;
		changedetectOperator.outputName = "output";
		
		return changedetectOperator;
	}
}