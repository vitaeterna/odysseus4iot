package odysseus4iot.graph.operator.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatabasesinkOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.Schema;
import odysseus4iot.main.Main;
import odysseus4iot.model.Feature;

/**
 * The {@code OperatorGenerator} provides methods to generate specific {@link Operator} objects.
 * 
 * @author Michael Sünkel
 */
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
		//attributes.addColumn(new Column("timestamp", StartTimestamp.class));
		
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
		
		mergeOperator.inputStreams = new ArrayList<>(inputStreams);
		
		mergeOperator.outputSchema = null;
		mergeOperator.outputRate = null;
		mergeOperator.outputName = "merge_" + MergeOperator.getNextMergeCount();
		
		return mergeOperator;
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
		timewindowOperator.outputName = "sensor_data_map_window_" + size;
		
		return timewindowOperator;
	}
	
	public static AggregateOperator generateAggregateOperator(List<Feature> features, String suffix)
	{
		AggregateOperator aggregateOperator = new AggregateOperator();
		
		aggregateOperator.group_by = "cattle_id";
		
		List<String> aggregations = new ArrayList<>();
		
		Feature currentFeature = null;
		String[] currentFeatureSplit = null;
		
		Collections.sort(features);
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			currentFeatureSplit = currentFeature.name.split("_");
			
			aggregations.add("['" + currentFeatureSplit[1] + "', '" + currentFeatureSplit[0] + "', '" + currentFeature.name + suffix + "', '" + currentFeature.type.toUpperCase() + "']");
		}
		
		aggregateOperator.aggregations = aggregations;
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		
		for(int index = 0; index < features.size(); index++)
		{
			currentFeature = features.get(index);
			
			outputSchema.addColumn(new Column(currentFeature + suffix, Double.class));
		}
		
		aggregateOperator.outputSchema = outputSchema;
		aggregateOperator.outputRate = null;
		aggregateOperator.outputName = null;
		
		return aggregateOperator;
	}
	
	public static ProjectOperator generateProjectOperator(List<Feature> attributes, String suffix)
	{
		ProjectOperator projectOperator = new ProjectOperator();
		
		Collections.sort(attributes);
		
		projectOperator.payloadAttributes = Feature.getNamesFromFeatures(attributes);
		
		projectOperator.attributes = new ArrayList<>();
		projectOperator.attributes.add("'cattle_id'");
		
		Feature currentAttribute = null;
		
		for(int index = 0; index < attributes.size(); index++)
		{
			currentAttribute = attributes.get(index);
			
			if(suffix != null)
			{
				projectOperator.attributes.add("'" + currentAttribute.name + suffix + "'");
			}
			else
			{
				projectOperator.attributes.add("'" + currentAttribute.name + "'");
			}
		}
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		
		for(int index = 0; index < attributes.size(); index++)
		{
			currentAttribute = attributes.get(index);
			
			if(suffix != null)
			{
				outputSchema.addColumn(new Column(currentAttribute.name + suffix, Double.class));
			}
			else
			{
				outputSchema.addColumn(new Column(currentAttribute.name, Double.class));
			}
		}
		
		projectOperator.outputSchema = outputSchema;
		projectOperator.outputRate = null;
		projectOperator.outputName = "project_" + ProjectOperator.getNextProjectCount();
		
		return projectOperator;
	}
	
	public static ClassificationOperator generateClassificationOperator(String model_title, String rpcServerSocket)
	{
		ClassificationOperator classificationOperator = new ClassificationOperator();
		
		classificationOperator.database = Main.properties.getProperty("modeldb.database");
		classificationOperator.host = Main.properties.getProperty("modeldb.host");
		classificationOperator.port = Integer.parseInt(Main.properties.getProperty("modeldb.port"));
		classificationOperator.rpcServer = rpcServerSocket;
		classificationOperator.table = Main.properties.getProperty("modeldb.table");
		classificationOperator.username = Main.properties.getProperty("modeldb.user");
		classificationOperator.password = Main.properties.getProperty("modeldb.password");
		classificationOperator.selectmodelbycolumn = Main.properties.getProperty("modeldb.column");
		classificationOperator.selectmodelbyvalue = model_title;
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("activity", String.class));
		
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
		
		changedetectOperator.attr = "activity";
		changedetectOperator.group_by = "cattle_id";
		
		changedetectOperator.outputSchema = null;
		changedetectOperator.outputRate = null;
		changedetectOperator.outputName = "classification_" + model_title + "_final";
		
		return changedetectOperator;
	}
	
	public static DatabasesinkOperator generateDatabasesinkOperator(String table)
	{
		DatabasesinkOperator databasesinkOperator = new DatabasesinkOperator();
		
		databasesinkOperator.table = table;
		databasesinkOperator.jdbc = Main.properties.getProperty("predictiondb.url");
		databasesinkOperator.user = Main.properties.getProperty("predictiondb.user");
		databasesinkOperator.password = Main.properties.getProperty("predictiondb.password");
		
		databasesinkOperator.outputSchema = null;
		databasesinkOperator.outputRate = null;
		databasesinkOperator.outputName = "activity_sink";
		
		return databasesinkOperator;
	}
	
	public static SenderOperator generateSenderOperator(String host, Integer port)
	{
		SenderOperator senderOperator = new SenderOperator();
		senderOperator.host = host;
		senderOperator.port = port;
		
		senderOperator.outputSchema = null;
		senderOperator.outputRate = null;
		senderOperator.outputName = null;
		
		return senderOperator;
	}
	
	public static AccessOperator generateAccessOperator(String host, Integer port)
	{
		AccessOperator accessOperator = new AccessOperator();
		accessOperator.host = host;
		accessOperator.port = port;
		
		accessOperator.outputSchema = null;
		accessOperator.outputRate = null;
		accessOperator.outputName = null;
		
		return accessOperator;
	}
	
	public static DatarateOperator generateDatarateOperator(String key)
	{
		DatarateOperator datarateOperator = new DatarateOperator();
		datarateOperator.key = key;
		
		datarateOperator.outputSchema = null;
		datarateOperator.outputRate = null;
		datarateOperator.outputName = datarateOperator.key;
		
		return datarateOperator;
	}
}