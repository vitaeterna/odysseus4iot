package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.placement.model.OperatorPlacement;
import odysseus4iot.util.Util;

public class OperatorPlacementOptimization
{
	private static Integer minID = null;
	private static Integer maxID = null;
	private static List<Operator> operators = null;
	private static Integer placementCounter = null;
	private static Integer placementSearchSpaceSize = null;
	private static List<OperatorPlacement> operatorPlacements = null;
	
	public static List<OperatorPlacement> optimize(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		minID = physicalGraph.getMinVertexID();
		maxID = physicalGraph.getMaxVertexID();
		
		Operator currentOperator = null;
		
		operators = new ArrayList<>();
		
		for(int index = 0; index < operatorGraph.vertices.size(); index++)
		{
			currentOperator = (Operator)operatorGraph.vertices.get(index);
			
			if(currentOperator.type.equals(Operator.Type.SOURCE))
			{
				currentOperator.assignedID = minID;
			}
			else if(currentOperator.type.equals(Operator.Type.SINK))
			{
				currentOperator.assignedID = maxID;
			}
			else
			{
				operators.add(currentOperator);
			}
		}
		
		placementCounter = 0;
		placementSearchSpaceSize = (int) Math.pow(maxID, operators.size());
		
		operatorPlacements = new ArrayList<>();
		
		System.out.println("Operator Placement Optimization started...");
		System.out.println("Physical Nodes:     " + maxID);
		System.out.println("Operators:          " + operatorGraph.vertices.size());
		System.out.println("    - Dynamic:      " + operators.size());
		System.out.println("    - Static:       " + (operatorGraph.vertices.size() - operators.size()) + " (fixed physical node assignment for sources and sinks)");
		System.out.println("Search Space Size:  " + placementSearchSpaceSize + " (" + maxID + "^" + operators.size() + ")");
		
		Util.printProgressBar(placementCounter, placementSearchSpaceSize);
		
		while(nextPlacement())
		{
			if(operatorGraph.allOperatorsPlaced())
			{
				if(operatorGraph.allDataFlowsValid(physicalGraph))
				{
					if(physicalGraph.allNodeCapacitiesFine(operatorGraph))
					{
						if(physicalGraph.allConnectionCapacitiesFine(operatorGraph))
						{
							OperatorPlacement operatorPlacement = new OperatorPlacement();
							operatorPlacement.placement = printPlacementOnVertexList(operatorGraph.vertices);
							operatorPlacement.id = placementCounter;
							
							operatorPlacement.datarateTotal = operatorGraph.getTotalDatarate(physicalGraph);
							operatorPlacement.numberOfConnections = operatorGraph.getNumberOfConnections();
							operatorPlacement.numberOfEdgeOperators = operatorGraph.getNumberOfEdgeOperators(physicalGraph);
							
							operatorPlacements.add(operatorPlacement);
							
							Util.printProgressBar(placementCounter, placementSearchSpaceSize);
						}
					}
				}
			}
		}
		
		Util.printProgressBar(placementCounter, placementSearchSpaceSize);
		
		System.out.println("\nValid Operator Placements found: " + operatorPlacements.size());
		
		Collections.sort(operatorPlacements);
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Operator Placement Optimization finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	
		return operatorPlacements;
	}
	
	private static boolean nextPlacement()
	{
		Operator currentOperator = null;
		
		if(placementCounter == 0)
		{
			for(int index = 0; index < operators.size(); index++)
			{
				currentOperator = operators.get(index);
				
				currentOperator.assignedID = minID;
			}
			
			placementCounter++;
			
			return true;
		}
		else
		{
			for(int index = 0; index < operators.size(); index++)
			{
				currentOperator = operators.get(index);
				
				if(currentOperator.assignedID < maxID)
				{
					currentOperator.assignedID++;
					
					placementCounter++;
					
					return true;
				}
				else
				{
					currentOperator.assignedID = minID;
				}
			}
			
			return false;
		}
	}
	
	private static String printPlacementOnVertexList(List<Vertex> vertices)
	{
		StringBuilder stringBuilder = new StringBuilder();
		
		Operator currentOperator = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentOperator = (Operator)vertices.get(index);
			
			stringBuilder.append(currentOperator.assignedID + ((index==vertices.size()-1)?"":"|"));
		}
		
		return stringBuilder.toString();
	}
}