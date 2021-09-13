package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class TimewindowOperator extends Operator
{
	public Integer size = null;
	public Integer slide = null;
	
	public TimewindowOperator()
	{
		super();
		
		type = Type.BOX;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.size, this.slide, this.inputName);
	}
	
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