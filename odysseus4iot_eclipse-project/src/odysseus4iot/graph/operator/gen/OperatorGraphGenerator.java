package odysseus4iot.graph.operator.gen;

import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.MapOperator;
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
			
			databasesourceOperator = OperatorGenerator.generateDatabasesourceOperator(currentSensor, model.getSchema(), 100);
			
			graph.addVertex(databasesourceOperator);
		}
		
		//2 - MapOperator
		MapOperator mapOperator = OperatorGenerator.generateMapOperator(model.getPreprocessing());
		
		graph.addVertex(mapOperator);
		
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
		mapOperator.inputName = currentOperator.outputName; //TODO: this needs to be adjusted to several inputs/MERGE for all in one node

		mapOperator.outputRate = mapOperator.inputRate;
		
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
		AggregateOperator aggregateOperator = OperatorGenerator.generateAggregateOperator();
		
		//TODO: set all input informations regarding pre-output info
		
		return graph;
	}
}