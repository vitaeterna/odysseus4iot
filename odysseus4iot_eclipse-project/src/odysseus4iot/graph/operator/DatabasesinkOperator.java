package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

//TODO
public class DatabasesinkOperator extends Operator
{
	public DatabasesinkOperator()
	{
		super();
		
		type = Type.SINK;
	}
	
	@Override
	public String toString()
	{
		return QUERY;
	}
	
	private static final String QUERY = 
			  "%s = DATABASESOURCE\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\ttable='%s',\r\n"
			+ "\t\tjdbc='%s',\r\n"
			+ "\t\tuser='%s',\r\n"
			+ "\t\tpassword='%s',\r\n"
			+ "\t\tconnection='connection',\r\n"
			+ "\t\tattributes=\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t],\r\n"
			+ "\t\twaiteach=%d\r\n"
			+ "\t}\r\n"
			+ ")";
}