package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class OutlierRemovingOperator extends Operator
{
	public String group_by = null;
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.group_by, this.inputName, this.group_by, this.outputName);
	}
	
	public static final String QUERY = 
			  "elements = ELEMENTWINDOW({SIZE = 2, ADVANCE = 1, DRAINATDONE = true, PARTITION = ['%s']}, %s)\r\n"
			+ "\r\n"
			+ "aggregation = AGGREGATE({AGGREGATIONS = [['nest', '*', 'nested']], GROUP_BY = ['%s']}, elements)\r\n"
			+ "\r\n"
			+ "filter = SELECT({PREDICATE = 'size(nested) = 2 && elementAt(nested[0],1) = elementAt(nested[1],1)'}, aggregation)\r\n"
			+ "\r\n"
			+ "mapped = MAP({EXPRESSIONS = [['nested[0]','nested']]}, filter)\r\n"
			+ "\r\n"
			+ "%s = UNNEST({ATTRIBUTE = 'nested'}, mapped)";
}