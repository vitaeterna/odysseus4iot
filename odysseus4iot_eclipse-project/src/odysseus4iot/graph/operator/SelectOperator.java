package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class SelectOperator extends Operator
{
	private static Integer selectCount = 0;
	
	public static Integer getCurrentSelectCount()
	{
		return selectCount;
	}
	
	public static Integer getNextSelectCount()
	{
		return ++selectCount;
	}
	
	public static void resetSelectCount()
	{
		selectCount = 0;
	}
	
	public SelectOperator()
	{
		super();
		
		type = Type.NOP;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = SELECT\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tpredicate='true'\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}