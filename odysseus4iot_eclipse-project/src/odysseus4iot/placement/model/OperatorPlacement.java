package odysseus4iot.placement.model;

import odysseus4iot.util.Util;

public class OperatorPlacement implements Comparable<OperatorPlacement>
{
	public Integer id = null;
	public String placement = null;
	public Double datarateTotal = null;
	
	@Override
	public int compareTo(OperatorPlacement operatorPlacement)
	{
		return datarateTotal.compareTo(operatorPlacement.datarateTotal);
	}

	@Override
	public String toString()
	{
		return placement + " - " + Util.formatDatarate(datarateTotal);
	}
}