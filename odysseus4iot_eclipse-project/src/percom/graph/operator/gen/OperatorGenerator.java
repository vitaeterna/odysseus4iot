package percom.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import percom.graph.operator.AggregateOperator;
import percom.graph.operator.ClassificationOperator;
import percom.graph.operator.DatabasesourceOperator;
import percom.graph.operator.MapOperator;
import percom.graph.operator.TimewindowOperator;
import percom.graph.operator.meta.Column;
import percom.graph.operator.meta.Schema;

//TODO: Warum kann integer timestamp nicht als starttimestamp verwendet werden? Muss timestamp in String vorliegen?
//TODO: Werden metadaten, wie start und end, immer mitübertragen, auch wenn diese null sind?
//TODO: Wie können Input-Columns bei Aggregate einfach durchgereicht werden?
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
		outputSchema.addColumn(new Column("amag_count", Integer.class));
		outputSchema.addColumn(new Column("amag_min", Double.class));
		outputSchema.addColumn(new Column("omag_min", Double.class));
		outputSchema.addColumn(new Column("amag_max", Integer.class));
		outputSchema.addColumn(new Column("omag_max", Double.class));
		outputSchema.addColumn(new Column("amag_avg", Double.class));
		outputSchema.addColumn(new Column("omag_avg", Double.class));
		outputSchema.addColumn(new Column("amag_median", Double.class));
		outputSchema.addColumn(new Column("omag_median", Double.class));
		outputSchema.addColumn(new Column("amag_stddev", Double.class));
		outputSchema.addColumn(new Column("omag_stddev", Double.class));
		outputSchema.addColumn(new Column("amag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("omag_kurtosis", Double.class));
		outputSchema.addColumn(new Column("amag_skewness", Double.class));
		outputSchema.addColumn(new Column("omag_skewness", Double.class));
		outputSchema.addColumn(new Column("amag_iqr", Double.class));
		outputSchema.addColumn(new Column("omag_iqr", Double.class));
		outputSchema.addColumn(new Column("amag_mcr", Double.class));
		outputSchema.addColumn(new Column("omag_mcr", Double.class));
		outputSchema.addColumn(new Column("amag_rms", Double.class));
		outputSchema.addColumn(new Column("omag_rms", Double.class));
		outputSchema.addColumn(new Column("amag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("omag_FrEnergy", Double.class));
		outputSchema.addColumn(new Column("amag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("omag_FrDmEntroPy", Double.class));
		outputSchema.addColumn(new Column("amag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("omag_FrPeakFreq", Double.class));
		outputSchema.addColumn(new Column("amag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("amag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("amag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("amag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("amag_FrMag5", Double.class));
		outputSchema.addColumn(new Column("omag_FrMag1", Double.class));
		outputSchema.addColumn(new Column("omag_FrMag2", Double.class));
		outputSchema.addColumn(new Column("omag_FrMag3", Double.class));
		outputSchema.addColumn(new Column("omag_FrMag4", Double.class));
		outputSchema.addColumn(new Column("omag_FrMag5", Double.class));
		
		aggregateOperator.outputSchema = outputSchema;
		aggregateOperator.outputRate = 0.2d;
		aggregateOperator.outputName = "features";
		
		return aggregateOperator;
	}
	
	public ClassificationOperator generateClassificationOperator()
	{
		ClassificationOperator classificationOperator = new ClassificationOperator();
		
		//TODO
		
		return classificationOperator;
	}
}