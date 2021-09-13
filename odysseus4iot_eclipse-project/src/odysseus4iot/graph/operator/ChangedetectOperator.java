package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class ChangedetectOperator extends Operator
{
	public String attr = null;
	public String group_by = null;
	
	public ChangedetectOperator()
	{
		super();
		
		type = Type.BOX;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.attr, this.group_by, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = CHANGEDETECT\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tattr = ['%s'],\r\n"
			+ "\t\tgroup_by = ['%s'],\r\n"
			+ "\t\tdeliverFirstElement = true\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}