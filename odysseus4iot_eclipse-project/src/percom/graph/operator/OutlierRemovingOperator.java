package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class OutlierRemovingOperator extends Operator
{
	public static final String QUERY = 
			  "elements = ELEMENTWINDOW({SIZE = 2, ADVANCE = 1, DRAINATDONE = true, PARTITION = ['id']}, System.labels)\r\n"
			+ "\r\n"
			+ "aggregation = AGGREGATE({AGGREGATIONS = [['nest', '*', 'nested']], GROUP_BY = ['id']}, elements)\r\n"
			+ "\r\n"
			+ "filter = SELECT({PREDICATE = 'size(nested) = 2 && elementAt(nested[0],2) = elementAt(nested[1],2)'}, aggregation)\r\n"
			+ "\r\n"
			+ "mapped = MAP({EXPRESSIONS = [['nested[0]','nested']]}, filter)\r\n"
			+ "\r\n"
			+ "unnest = UNNEST({ATTRIBUTE = 'nested'}, mapped)";
}