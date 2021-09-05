package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class TimewindowOperator extends Operator
{
	public static final String QUERY = 
			  "sensorMagWindowed = TIMEWINDOW\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tsize=5000,\r\n"
			+ "\t\tslide=5000\r\n"
			+ "\t},\r\n"
			+ "\tsensorMagnitudes\r\n"
			+ ")";
}