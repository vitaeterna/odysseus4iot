package odysseus4iot.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.Schema;
import odysseus4iot.main.Main;

//TODO: Warum kann integer timestamp nicht als starttimestamp verwendet werden? Muss timestamp in String vorliegen?
public class OperatorGenerator
{
	public static DatabasesourceOperator generateDatabasesourceOperator(String sensor, List<String> schema, Double waiteach)
	{
		DatabasesourceOperator databasesourceOperator = new DatabasesourceOperator();
		
		databasesourceOperator.table = sensor;
		databasesourceOperator.jdbc = Main.properties.getProperty("sensordb.url");
		databasesourceOperator.user = Main.properties.getProperty("sensordb.user");
		databasesourceOperator.password = Main.properties.getProperty("sensordb.password");
		databasesourceOperator.waiteach = waiteach.intValue();
		
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
		databasesourceOperator.outputRate = 1000.0d/databasesourceOperator.waiteach;
		databasesourceOperator.outputName = sensor;
		
		return databasesourceOperator;
	}
	
	public static MergeOperator generateMergeOperator(List<String> inputStreams)
	{
		MergeOperator mergeOperator = new MergeOperator();
		
		mergeOperator.inputStreams = inputStreams;
		
		mergeOperator.outputSchema = null;
		mergeOperator.outputRate = null;
		mergeOperator.outputName = "sensor_data";
		
		return null;
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
	
	public static AggregateOperator generateAggregateOperator(List<String> features)
	{
		AggregateOperator aggregateOperator = new AggregateOperator();
		
		aggregateOperator.group_by = "cattle_id";
		
		List<String> aggregations = new ArrayList<>();
		
		String currentFeature = null;
		String[] currentFeatureSplit = null;
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			currentFeatureSplit = currentFeature.split("_");
			
			aggregations.add("['" + currentFeatureSplit[1] + "', ['" + currentFeatureSplit[0] + "'], '" + currentFeature + "', 'Double']");
		}
		
		aggregateOperator.aggregations = aggregations;
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			
			outputSchema.addColumn(new Column("car_" + currentFeature, Double.class));
		}
		
		aggregateOperator.outputSchema = outputSchema;
		aggregateOperator.outputRate = null;
		aggregateOperator.outputName = "features";
		
		return aggregateOperator;
	}
	
	public static ProjectOperator generateProjectOperator(String model_title, List<String> features)
	{
		ProjectOperator projectOperator = new ProjectOperator();
		
		List<String> attributes = new ArrayList<>();
		attributes.add("'cattle_id'");
		
		String currentFeature = null;
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			
			attributes.add("car_" + currentFeature);
		}
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			
			outputSchema.addColumn(new Column("car_" + currentFeature, Double.class));
		}
		
		projectOperator.outputSchema = outputSchema;
		projectOperator.outputRate = null;
		projectOperator.outputName = "features_" + model_title;
		
		return projectOperator;
	}
	
	public static ClassificationOperator generateClassificationOperator(String model_title)
	{
		ClassificationOperator classificationOperator = new ClassificationOperator();
		
		classificationOperator.database = Main.properties.getProperty("modeldb.database");
		classificationOperator.host = Main.properties.getProperty("modeldb.host");
		classificationOperator.port = Integer.parseInt(Main.properties.getProperty("modeldb.port"));
		classificationOperator.rpcServer = Main.properties.getProperty("pythonrpc.socket"); //TODO: several rpc servers? Odysseus Java prediction
		classificationOperator.table = Main.properties.getProperty("modeldb.table");
		classificationOperator.username = Main.properties.getProperty("modeldb.user");
		classificationOperator.password = Main.properties.getProperty("modeldb.password");
		classificationOperator.selectmodelbycolumn = Main.properties.getProperty("modeldb.column");
		classificationOperator.selectmodelbyvalue = model_title;
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column(model_title, String.class));
		
		classificationOperator.outputSchema = outputSchema;
		classificationOperator.outputRate = null;
		classificationOperator.outputName = "classification_" + model_title;
		
		return classificationOperator;
	}
	
	public static OutlierRemovingOperator generateOutlierRemovingOperator(String model_title)
	{
		OutlierRemovingOperator outlierRemovingOperator = new OutlierRemovingOperator();
		
		outlierRemovingOperator.group_by = "cattle_id";
		
		outlierRemovingOperator.model_title = model_title;
		
		outlierRemovingOperator.outputSchema = null;
		outlierRemovingOperator.outputRate = null;
		outlierRemovingOperator.outputName = "classification_" + model_title + "_outlier";
		
		return outlierRemovingOperator;
	}
	
	public static ChangedetectOperator generateChangedetectOperator(String model_title)
	{
		ChangedetectOperator changedetectOperator = new ChangedetectOperator();
		
		changedetectOperator.attr = model_title;
		changedetectOperator.group_by = "cattle_id";
		
		changedetectOperator.outputSchema = null;
		changedetectOperator.outputRate = null;
		changedetectOperator.outputName = "classification_" + model_title + "_final";
		
		return changedetectOperator;
	}
}