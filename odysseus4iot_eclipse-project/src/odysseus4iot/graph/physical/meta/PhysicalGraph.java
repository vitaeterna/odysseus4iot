package odysseus4iot.graph.physical.meta;

import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Node.Type;

public class PhysicalGraph extends Graph
{
	public PhysicalGraph()
	{
		super();
	}
	
	public Type getNodeTypeByID(int id)
	{
		Node currentNode = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			if(currentNode.id.intValue() == id)
			{
				return currentNode.type;
			}
		}
		
		return null;
	}
	
	public boolean allNodeCapacitiesFine(OperatorGraph operatorGraph)
	{
		Node currentNode = null;
		
		Operator currentOperator = null;
		
		List<Operator> operators = null;
		
		Integer cpuConsumption = null;
		Integer memConsumption = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentNode = (Node)vertices.get(index);
			
			operators = operatorGraph.getOperatorsByAssignedID(currentNode.id);
		
			cpuConsumption = 0;
			memConsumption = 0;
			
			for(int index2 = 0; index2 < operators.size(); index2++)
			{
				currentOperator = operators.get(index2);
				
				cpuConsumption += currentOperator.cpuConsumption;
				memConsumption += currentOperator.memConsumption;
			}
			
			if(currentNode.cpuCapacity < cpuConsumption || currentNode.memCapacity < memConsumption)
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
			
			if(currentConnection.datarateCapacity < datarateConsumption)
			{
				return false;
			}
		}
		
		return true;
	}
}