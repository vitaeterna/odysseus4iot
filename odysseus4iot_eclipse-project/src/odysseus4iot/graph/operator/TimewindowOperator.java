package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.main.Main;

public class TimewindowOperator extends Operator
{
	public Integer size = null;
	public Integer slide = null;
	
	public TimewindowOperator()
	{
		super();
		
		type = Type.PROCESSING;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, (int)(this.size/Main.evaluationSpeedupFactor), (int)(this.slide/Main.evaluationSpeedupFactor), this.inputName);
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