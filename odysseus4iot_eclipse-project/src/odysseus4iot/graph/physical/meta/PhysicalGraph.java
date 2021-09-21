package odysseus4iot.graph.physical.meta;

import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Node.Type;

public class PhysicalGraph extends Graph
{
	public PhysicalGraph(String label)
	{
		super(label);
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
	
	public Long getMemConsumptionEdge()
	{
		Node currentNode = null;
		
		Long memConsumptionEdge = 0L;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			if(currentNode.type.equals(Type.EDGE))
			{
				memConsumptionEdge += currentNode.memConsumed;
			}
		}
		
		return memConsumptionEdge;
	}
	
	public boolean allNodeCapacitiesFine(OperatorGraph operatorGraph)
	{
		Node currentNode = null;
		
		Operator currentOperator = null;
		
		List<Operator> operators = null;
		
		Long cpuConsumption = null;
		Long memConsumption = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			operators = operatorGraph.getOperatorsByAssignedID(currentNode.id);
		
			cpuConsumption = 0L;
			memConsumption = 0L;
			
			for(int index2 = 0; index2 < operators.size(); index2++)
			{
				currentOperator = operators.get(index2);
				
				cpuConsumption += currentOperator.cpuConsumption;
				memConsumption += currentOperator.memConsumption;
			}
			
			currentNode.cpuConsumed = cpuConsumption;
			currentNode.memConsumed = memConsumption;
			
			if((currentNode.cpuCapacity.longValue() != -1L && currentNode.cpuCapacity < currentNode.cpuConsumed) || (currentNode.memCapacity.longValue() != -1L && currentNode.memCapacity < currentNode.memConsumed))
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