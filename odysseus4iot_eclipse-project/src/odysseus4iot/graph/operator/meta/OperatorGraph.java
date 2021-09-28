package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Operator.Type;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.placement.model.OperatorPlacement;

public class OperatorGraph extends Graph
{
	public OperatorGraph(String label)
	{
		super(label);
		
		MergeOperator.resetMergeCount();
		ProjectOperator.resetProjectCount();
		SenderOperator.resetSenderCount();
		AccessOperator.resetAccessCount();
		DatarateOperator.resetDatarateCount();
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
				datarateTotal += currentDataFlow.datarateConsumption;
			}
		}
		
		return datarateTotal;
	}
	
	public Integer getNumberOfConnections()
	{
		Integer numberOfConnections = 0;
		
		DataFlow currentDataFlow = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentDataFlow = (DataFlow)edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID != operator1.assignedID)
			{
				numberOfConnections++;
			}
		}
		
		return numberOfConnections;
	}
	
	public Integer getNumberOfEdgeOperators(PhysicalGraph physicalGraph)
	{
		Integer numberOfEdgeOperators = 0;
		
		Operator currentOperator = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			if(currentOperator.type.equals(Type.PROCESSING) && physicalGraph.getNodeByID(currentOperator.assignedID).type.equals(Node.Type.EDGE))
			{
				numberOfEdgeOperators++;
			}
		}
		
		return numberOfEdgeOperators;
	}
	
	public List<Integer> getNumberOfOperatorsPerPipelineStep()
	{
		Integer numberOfPreprocessingOperators = 0;
		Integer numberOfSegmentationOperators = 0;
		Integer numberOfAggregationOperators = 0;
		Integer numberOfClassificationOperators = 0;
		Integer numberOfPostprocessingOperators = 0;
		
		Operator currentOperator = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			if(currentOperator.type.equals(Type.PROCESSING))
			{
				if(currentOperator instanceof MapOperator)
				{
					numberOfPreprocessingOperators++;
				}
				if(currentOperator instanceof TimewindowOperator)
				{
					numberOfSegmentationOperators++;
				}
				if(currentOperator instanceof AggregateOperator)
				{
					numberOfAggregationOperators++;
				}
				if(currentOperator instanceof ClassificationOperator)
				{
					numberOfClassificationOperators++;
				}
				if(currentOperator instanceof OutlierRemovingOperator || currentOperator instanceof ChangedetectOperator)
				{
					numberOfPostprocessingOperators++;
				}
			}
		}
		
		List<Integer> numberOfOperatorsPerPiplineStep = new ArrayList<>();
		
		numberOfOperatorsPerPiplineStep.add(numberOfPreprocessingOperators);
		numberOfOperatorsPerPiplineStep.add(numberOfSegmentationOperators);
		numberOfOperatorsPerPiplineStep.add(numberOfAggregationOperators);
		numberOfOperatorsPerPiplineStep.add(numberOfClassificationOperators);
		numberOfOperatorsPerPiplineStep.add(numberOfPostprocessingOperators);
		
		return numberOfOperatorsPerPiplineStep;
	}

	public boolean loadOperatorPlacement(OperatorPlacement operatorPlacement, PhysicalGraph physicalGraph)
	{
		String[] placementSplit = operatorPlacement.placement.split("\\|");
		
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
		
		boolean allOperatorsPlaced = this.allOperatorsPlaced();
		boolean allDataFlowsValid = this.allDataFlowsValid(physicalGraph);
		boolean allNodeCapacitiesFine = physicalGraph.allNodeCapacitiesFine(this);
		boolean allConnectionCapacitiesFine = physicalGraph.allConnectionCapacitiesFine(this);
		
		if(allOperatorsPlaced && allDataFlowsValid && allNodeCapacitiesFine && allConnectionCapacitiesFine)
		{
			this.setLabels();
			
			physicalGraph.setLabels();
			
			this.label = "placement_" + operatorPlacement.id;
			
			return true;
		}
		
		return false;
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
	
	public boolean allDataFlowsValid(PhysicalGraph physicalGraph)
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
	
	public void setControlFlowDatarates()
	{
		DataFlow currentDataFlow = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)this.edges.get(index);
			
			currentDataFlow.datarateConsumption = ((Operator)currentDataFlow.vertex0).outputRate * ((Operator)currentDataFlow.vertex0).outputSchema.getSize();
		}
	}
}