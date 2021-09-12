package odysseus4iot.graph.operator.gen;

import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.model.Model;

public class OperatorGraphGenerator
{
	public static Graph generateOperatorGraph(List<String> sensors, Model model)
	{
		Graph graph = new Graph();
		
		//1 - DatabasesourceOperator
		DatabasesourceOperator databasesourceOperator = null;
		
		String currentSensor = null;
		
		for(int index = 0; index < sensors.size(); index++)
		{
			currentSensor = sensors.get(index);
			
			databasesourceOperator = OperatorGenerator.generateDatabasesourceOperator(currentSensor, model.getSchema(), model.getWaiteach());
			
			graph.addVertex(databasesourceOperator);
		}
		
		//1.5 - MergeOperator
		MergeOperator mergeOperator = null;
		
		if(sensors.size() > 1)
		{
			mergeOperator = OperatorGenerator.generateMergeOperator(sensors);
			
			graph.addVertex(mergeOperator);
			
			List<Vertex> vertices = graph.getVerticesByType(DatabasesourceOperator.class);
			
			Operator currentOperator = null;
			
			Double outputRateSum = 0.0d;
			
			for(int index = 0; index < vertices.size(); index++)
			{
				currentOperator = (Operator)vertices.get(index);
				
				graph.addEdge(new Edge(currentOperator, mergeOperator));
				
				outputRateSum += currentOperator.outputRate;
			}
			
			mergeOperator.inputSchema = currentOperator.outputSchema.copy();
			mergeOperator.inputRate = outputRateSum;
			mergeOperator.inputName = null;

			mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
			mergeOperator.outputRate = mergeOperator.inputRate;
		}
		
		//2 - MapOperator
		MapOperator mapOperator = OperatorGenerator.generateMapOperator(model.getPreprocessing());
		
		graph.addVertex(mapOperator);
		
		if(mergeOperator == null)
		{
			List<Vertex> vertices = graph.getVerticesByType(DatabasesourceOperator.class);
			
			Operator currentOperator = null;
			
			Double outputRateSum = 0.0d;
			
			for(int index = 0; index < vertices.size(); index++)
			{
				currentOperator = (Operator)vertices.get(index);
				
				graph.addEdge(new Edge(currentOperator, mapOperator));
				
				outputRateSum += currentOperator.outputRate;
			}
			
			mapOperator.inputSchema = currentOperator.outputSchema.copy();
			mapOperator.inputRate = outputRateSum;
			mapOperator.inputName = currentOperator.outputName;
	
			mapOperator.outputRate = mapOperator.inputRate;
		}
		else
		{
			graph.addEdge(new Edge(mergeOperator, mapOperator));
			
			mapOperator.inputSchema = mergeOperator.outputSchema.copy();
			mapOperator.inputRate = mergeOperator.outputRate;
			mapOperator.inputName = mergeOperator.outputName;
	
			mapOperator.outputRate = mapOperator.inputRate;
		}
		
		//3 - TimewindowOperator
		TimewindowOperator timewindowOperator = OperatorGenerator.generateTimewindowOperator(model.getWindow_size(), model.getWindow_slide());
		
		graph.addVertex(timewindowOperator);
		
		graph.addEdge(new Edge(mapOperator, timewindowOperator));
		
		timewindowOperator.inputSchema = mapOperator.outputSchema.copy();
		timewindowOperator.inputRate = mapOperator.outputRate;
		timewindowOperator.inputName = mapOperator.outputName;
		
		timewindowOperator.outputSchema = timewindowOperator.inputSchema.copy();
		timewindowOperator.outputRate = timewindowOperator.inputRate;
		
		//5 - AggregateOperator
		AggregateOperator aggregateOperator = OperatorGenerator.generateAggregateOperator(model.getFeatures());
		
		graph.addVertex(aggregateOperator);
		
		graph.addEdge(new Edge(timewindowOperator, aggregateOperator));
		
		aggregateOperator.inputSchema = timewindowOperator.outputSchema.copy();
		aggregateOperator.inputRate = timewindowOperator.outputRate;
		aggregateOperator.inputName = timewindowOperator.outputName;
		
		//TODO: consider window_slide
		aggregateOperator.outputRate = aggregateOperator.inputRate/(((double)model.getWindow_size())/model.getWaiteach().intValue());
		
		//6 - ClassificationOperator
		ClassificationOperator classificationOperator = OperatorGenerator.generateClassificationOperator(model.getModel_title());
		
		graph.addVertex(classificationOperator);
		
		graph.addEdge(new Edge(aggregateOperator, classificationOperator));
		
		classificationOperator.inputSchema = aggregateOperator.outputSchema.copy();
		classificationOperator.inputRate = aggregateOperator.outputRate;
		classificationOperator.inputName = aggregateOperator.outputName;
		
		classificationOperator.outputRate = classificationOperator.inputRate;
		
		//7 - OutlierRemovingOperator
		OutlierRemovingOperator outlierRemovingOperator = OperatorGenerator.generateOutlierRemovingOperator(model.getModel_title());
		
		graph.addVertex(outlierRemovingOperator);
		
		graph.addEdge(new Edge(classificationOperator, outlierRemovingOperator));
		
		outlierRemovingOperator.inputSchema = classificationOperator.outputSchema.copy();
		outlierRemovingOperator.inputRate = classificationOperator.outputRate;
		outlierRemovingOperator.inputName = classificationOperator.outputName;
		
		outlierRemovingOperator.outputSchema = outlierRemovingOperator.inputSchema.copy();
		outlierRemovingOperator.outputRate = outlierRemovingOperator.inputRate;
		
		//8 - ChangedetectOperator
		ChangedetectOperator changedetectOperator = OperatorGenerator.generateChangedetectOperator(model.getModel_title());
		
		graph.addVertex(changedetectOperator);
		
		graph.addEdge(new Edge(outlierRemovingOperator, changedetectOperator));
		
		changedetectOperator.inputSchema = outlierRemovingOperator.outputSchema.copy();
		changedetectOperator.inputRate = outlierRemovingOperator.outputRate;
		changedetectOperator.inputName = outlierRemovingOperator.outputName;
		
		changedetectOperator.outputSchema = changedetectOperator.inputSchema.copy();
		changedetectOperator.outputRate = null;
		
		return graph;
	}
}