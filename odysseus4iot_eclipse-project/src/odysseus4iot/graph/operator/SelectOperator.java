package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

/**
 * POJO representing the Odysseus Operator SELECT.
 * 
 * @author Michael Sünkel
 */
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
	
	public SelectOperator(Operator operator)
	{
		super(operator);
	}
	
	public SelectOperator copy()
	{
		SelectOperator operator = new SelectOperator(super.copy());
		
		return operator;
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