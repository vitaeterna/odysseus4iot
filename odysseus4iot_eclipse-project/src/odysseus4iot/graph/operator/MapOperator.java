package odysseus4iot.graph.operator;

import java.util.List;

import odysseus4iot.graph.operator.meta.Operator;

public class MapOperator extends Operator
{
	public List<String> expressions = null;
	
	@Override
	public String toString()
	{
		String expressionsString = "";
		
		String currentExpression = null;
		
		for(int index = 0; index < this.expressions.size(); index++)
		{
			currentExpression = this.expressions.get(index);
			
			expressionsString += String.format(QUERY_EXPRESSION, currentExpression, index==this.expressions.size()?"":",");
		}
		
		return String.format(QUERY, this.outputName, expressionsString, this.inputName);
	}
	
	private static final String QUERY = 
			  "%s = MAP\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\texpressions =\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t]\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
	
	private static final String QUERY_EXPRESSION = 
			  "\t\t\t%s%s\r\n";
}