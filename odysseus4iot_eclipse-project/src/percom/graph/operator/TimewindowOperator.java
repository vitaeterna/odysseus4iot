package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class TimewindowOperator extends Operator
{
	public Integer size = null;
	public Integer slide = null;
	
	public static final String QUERY = 
			  "%s = TIMEWINDOW\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tsize=%d,\r\n"
			+ "\t\tslide=%d\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}