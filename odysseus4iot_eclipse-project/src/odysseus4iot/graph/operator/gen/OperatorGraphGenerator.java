package odysseus4iot.graph.operator.gen;

import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.model.Model;

public class OperatorGraphGenerator
{
	public static Graph generateOperatorGraph(List<String> sensors, Model model)
	{
		Graph graph = new Graph();
		
		DatabasesourceOperator databasesourceOperator = null;
		
		String currentSensor = null;
		
		for(int index = 0; index < sensors.size(); index++)
		{
			currentSensor = sensors.get(index);
			
			databasesourceOperator = OperatorGenerator.generateDatabasesourceOperator(currentSensor, 100);
			System.out.println(databasesourceOperator+"\r\n");
		}
		
		return graph;
	}
}