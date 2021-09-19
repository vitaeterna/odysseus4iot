package odysseus4iot.graph.physical.meta;

import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;

public class PhysicalGraph extends Graph
{
	public PhysicalGraph()
	{
		super();
	}
	
	public Node getNodeByID(int id)
	{
		Node currentNode = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			if(currentNode.id.intValue() == id)
			{
				return currentNode;
			}
		}
		
		return null;
	}
	
	public boolean allNodeCapacitiesFine(OperatorGraph operatorGraph)
	{
		Node currentNode = null;
		
		Operator currentOperator = null;
		
		List<Operator> operators = null;
		
		Double cpuConsumption = null;
		Double memConsumption = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			operators = operatorGraph.getOperatorsByAssignedID(currentNode.id);
		
			cpuConsumption = 0.0d;
			memConsumption = 0.0d;
			
			for(int index2 = 0; index2 < operators.size(); index2++)
			{
				currentOperator = operators.get(index2);
				
				cpuConsumption += currentOperator.cpuConsumption;
				memConsumption += currentOperator.memConsumption;
			}
			
			currentNode.cpuConsumed = cpuConsumption;
			currentNode.memConsumed = memConsumption;
			
			if(currentNode.cpuCapacity < currentNode.cpuConsumed || currentNode.memCapacity < currentNode.memConsumed)
			{
				return false;
			}
		}
		
		return true;
	}
	
	public boolean allConnectionCapacitiesFine(OperatorGraph operatorGraph)
	{
		Connection currentConnection = null;
		
		DataFlow currentDataFlow = null;
		
		List<DataFlow> dataFlows = null;
		
		Double datarateConsumption = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentConnection = (Connection)edges.get(index);
			
			dataFlows = operatorGraph.getDataFlowsByAssignedIDs(currentConnection.vertex0.id, currentConnection.vertex1.id);
		
			datarateConsumption = 0.0d;
			
			for(int index2 = 0; index2 < dataFlows.size(); index2++)
			{
				currentDataFlow = dataFlows.get(index2);
				
				datarateConsumption += currentDataFlow.datarateConsumption;
			}
			
			currentConnection.datarateConsumed = datarateConsumption;
			
			if(currentConnection.datarateCapacity < currentConnection.datarateConsumed)
			{
				return false;
			}
		}
		
		return true;
	}
}