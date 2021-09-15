package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class DatabasesinkOperator extends Operator
{
	public String table = null;
	public String jdbc = null;
	public String user = null;
	public String password = null;
	
	public DatabasesinkOperator()
	{
		super();
		
		type = Type.SINK;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.table, this.jdbc, this.user, this.password, this.inputName);
	}
	
	private static final String QUERY = 
			  "%s = DATABASESINK\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\ttable='%s',\r\n"
			+ "\t\tjdbc='%s',\r\n"
			+ "\t\tuser='%s',\r\n"
			+ "\t\tpassword='%s',\r\n"
			+ "\t\tconnection='connection',\r\n"
			+ "\t\tdrop=false,\r\n"
			+ "\t\ttruncate=true,\r\n"
			+ "\t\tbatchsize=100,\r\n"
			+ "\t\tbatchtimeout=0,\r\n"
			+ "\t\trecoveryenabled=false,\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}