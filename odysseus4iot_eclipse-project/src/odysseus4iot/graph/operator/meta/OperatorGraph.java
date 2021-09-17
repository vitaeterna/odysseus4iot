package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.placement.model.OperatorPlacement;

public class OperatorGraph extends Graph
{
	public OperatorGraph()
	{
		super();
		
		MergeOperator.resetMergeCount();
		ProjectOperator.resetProjectCount();
	}
	
	public Double getTotalDatarate(PhysicalGraph physicalGraph)
	{
		DataFlow currentDataFlow = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		Double datarateTotal = 0.0d;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentDataFlow = (DataFlow)edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID != operator1.assignedID)
			{
				if(physicalGraph.getNodeTypeByID(operator0.assignedID).equals(Node.Type.EDGE))
				{
					datarateTotal += currentDataFlow.datarateConsumption * this.getStartingVertices().size();
				}
				else
				{
					datarateTotal += currentDataFlow.datarateConsumption;
				}
			}
		}
		
		return datarateTotal;
	}

	public void loadOperatorPlacement(OperatorPlacement operatorPlacement)
	{
		String[] placementSplit = operatorPlacement.placement.split("|");
		
		if(placementSplit.length != vertices.size())
		{
			System.err.println("The size of the operator placement definition (" + placementSplit.length + ") does not match the number of operators (" + vertices.size() + ")");
		
			System.exit(0);
		}
		
		Operator currentOperator = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			currentOperator.assignedID = Integer.parseInt(placementSplit[index]);
		}
	}
	
	public boolean allOperatorsPlaced()
	{
		Operator currentOperator = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			if(currentOperator.assignedID == null || currentOperator.assignedID < 1)
			{
				return false;
			}
		}
		
		return true;
	}
	
	public boolean allDataFlowsValid(Graph physicalGraph)
	{
		Edge currentEdge = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentEdge = edges.get(index);
			
			operator0 = (Operator)currentEdge.vertex0;
			operator1 = (Operator)currentEdge.vertex1;
			
			if(operator0.assignedID != operator1.assignedID)
			{
				if(!physicalGraph.containsEdge(operator0.assignedID, operator1.assignedID))
				{
					return false;
				}
			}
		}
		
		return true;
	}
	
	public List<DataFlow> getDataFlowsByAssignedIDs(int assignedID0, int assignedID1)
	{
		DataFlow currentDataFlow = null;

		List<DataFlow> dataFlows = new ArrayList<>();
		
		Operator operator0 = null;
		
		Operator operator1 = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentDataFlow = (DataFlow)edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID.intValue() == assignedID0 && operator1.assignedID.intValue() == assignedID1)
			{
				dataFlows.add(currentDataFlow);
			}
		}
		
		return dataFlows;
	}
	
	public List<Operator> getOperatorsByAssignedID(int assignedID)
	{
		Operator currentOperator = null;

		List<Operator> operators = new ArrayList<>();
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			if(currentOperator.assignedID.intValue() == assignedID)
			{
				operators.add(currentOperator);
			}
		}
		
		return operators;
	}
}