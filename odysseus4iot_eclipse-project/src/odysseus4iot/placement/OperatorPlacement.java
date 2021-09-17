package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.util.Util;

public class OperatorPlacement
{
	private static Integer minID = null;
	private static Integer maxID = null;
	private static List<Operator> operators = null;
	private static Integer placementCounter = null;
	private static Integer placementSize = null;
	
	public static void optimize(Graph operatorGraph, Graph physicalGraph)
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
		placementSize = (int) Math.pow(maxID, operators.size());
		
		System.out.println("Operator Placement Optimization started...");
		System.out.println("Physical Nodes:     " + maxID);
		System.out.println("Operators:          " + operatorGraph.vertices.size());
		System.out.println("    - Dynamic:      " + operators.size());
		System.out.println("    - Static:       " + (operatorGraph.vertices.size() - operators.size()) + " (fixed physical node assignment for sources and sinks)");
		System.out.println("Search Space Size:  " + placementSize + " (" + maxID + "^" + operators.size() + ")");
		
		while(nextPlacement())
		{
			System.out.println(printPlacement());
		}
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Operator Placement Optimization finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
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
			
			System.out.println(++placementCounter + "/" + placementSize + ": " + printPlacement());
			
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
					
					System.out.println(++placementCounter + "/" + placementSize + ": " + printPlacement());
					
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
	
	private static String printPlacement()
	{
		StringBuilder stringBuilder = new StringBuilder();
		
		Operator currentOperator = null;
		
		for(int index = 0; index < operators.size(); index++)
		{
			currentOperator = operators.get(index);
			
			stringBuilder.append(currentOperator.assignedID + ((index==operators.size()-1)?"":"|"));
		}
		
		return stringBuilder.toString();
	}
}