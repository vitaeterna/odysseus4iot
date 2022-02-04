package odysseus4iot.placement.model;

import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.util.Util;

/**
 * The {@code OperatorPlacement} represents a single mapping of operators to nodes.
 * 
 * @author Michael Sünkel
 */
public class OperatorPlacement implements Comparable<OperatorPlacement>
{
	public Integer id = null;
	public String placement = null;
	
	public Double datarateTotal = null;
	public Integer numberOfConnections = null;
	public Integer numberOfEdgeOperators = null;
	public Long memConsumptionEdge = null;
	
	public OperatorGraph operatorGraph = null;
	public PhysicalGraph physicalGraph = null;
	
	@Override
	public int compareTo(OperatorPlacement operatorPlacement)
	{
		if(datarateTotal.doubleValue() != operatorPlacement.datarateTotal)
		{
			return datarateTotal.compareTo(operatorPlacement.datarateTotal);
		}
		else
		{
			if(numberOfConnections.intValue() != operatorPlacement.numberOfConnections.intValue())
			{
				return numberOfConnections.compareTo(operatorPlacement.numberOfConnections);
			}
			else
			{
				if(numberOfEdgeOperators.intValue() != operatorPlacement.numberOfEdgeOperators.intValue())
				{
					return numberOfEdgeOperators.compareTo(operatorPlacement.numberOfEdgeOperators);
				}
				else
				{
					return memConsumptionEdge.compareTo(operatorPlacement.memConsumptionEdge);
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "placement_" + id + "_" + operatorGraph.label + " - DR=" + Util.formatSizeInBits(datarateTotal) + "/s | #Connections=" + numberOfConnections + " | #EdgeOperators=" + numberOfEdgeOperators + " | memConsumptionEdge=" + Util.formatSizeInBits((double)memConsumptionEdge);
	}
}