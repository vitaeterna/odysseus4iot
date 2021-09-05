package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class ChangedetectOperator extends Operator
{
	public static final String QUERY = 
			  "output = CHANGEDETECT\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tattr = ['label'],\r\n"
			+ "\t\tgroup_by = ['id'],\r\n"
			+ "\t\tdeliverFirstElement = true\r\n"
			+ "\t},\r\n"
			+ "\tunnest\r\n"
			+ ")";
}