package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

/**
 * POJO representing the Odysseus Operator DATABASESINK.
 * 
 * @author Michael Sünkel
 */
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
	
	public DatabasesinkOperator(Operator operator)
	{
		super(operator);
	}
	
	public DatabasesinkOperator copy()
	{
		DatabasesinkOperator operator = new DatabasesinkOperator(super.copy());
		
		operator.table = this.table;
		operator.jdbc = this.jdbc;
		operator.user = this.user;
		operator.password = this.password;
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.table, this.jdbc, this.user, this.password, this.id, this.inputName);
	}
	
	private static final String QUERY = 
			  "%s = DATABASESINK\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\ttable='%s',\r\n"
			+ "\t\tjdbc='%s',\r\n"
			+ "\t\tuser='%s',\r\n"
			+ "\t\tpassword='%s',\r\n"
			+ "\t\tconnection='connection%d',\r\n"
			+ "\t\tdrop=true,\r\n"
			+ "\t\ttruncate=false,\r\n"
			+ "\t\tbatchsize=50,\r\n"
			+ "\t\tbatchtimeout=10000,\r\n"
			+ "\t\trecoveryenabled=false\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}