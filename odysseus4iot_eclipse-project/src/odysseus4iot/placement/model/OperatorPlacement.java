package odysseus4iot.placement.model;

import odysseus4iot.util.Util;

public class OperatorPlacement implements Comparable<OperatorPlacement>
{
	public Integer id = null;
	public String placement = null;
	
	public Double datarateTotal = null;
	public Integer numberOfConnections = null;
	public Integer numberOfEdgeOperators = null;
	
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
				return numberOfEdgeOperators.compareTo(operatorPlacement.numberOfEdgeOperators);
			}
		}
	}

	@Override
	public String toString()
	{
		return "placement_" + id + " - DR=" + Util.formatDatarate(datarateTotal) + " | #Connections=" + numberOfConnections + " | #EdgeOperators=" + numberOfEdgeOperators;
	}
}