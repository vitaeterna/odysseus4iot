package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class SleepOperator extends Operator
{
	public Integer time = null;
	
	public SleepOperator(Integer time)
	{
		super();
		
		type = Type.BENCHMARK;
		
		this.time = time;
	}
	
	public SleepOperator(Operator operator)
	{
		super(operator);
	}
	
	public SleepOperator copy()
	{
		SleepOperator operator = new SleepOperator(super.copy());
		
		operator.time = this.time;
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.time, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = SLEEP\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\ttime=%d\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}