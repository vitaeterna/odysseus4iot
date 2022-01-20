package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class CalcLatencyOperator extends Operator
{
	public CalcLatencyOperator()
	{
		super();
		
		type = Type.BENCHMARK;
	}
	
	public CalcLatencyOperator(Operator operator)
	{
		super(operator);
	}
	
	public CalcLatencyOperator copy()
	{
		CalcLatencyOperator operator = new CalcLatencyOperator(super.copy());
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.inputName);
	}
	
	public static final String QUERY = 
			"%s = CALCLATENCY\r\n"
			+ "(\r\n"
			+ "\t%s\r\n"
			+ ")";
}