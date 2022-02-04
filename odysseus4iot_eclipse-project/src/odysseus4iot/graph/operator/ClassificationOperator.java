package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

/**
 * POJO representing the Odysseus Operator ACTIVITYCLASSIFY.
 * 
 * @author Michael Sünkel
 */
public class ClassificationOperator extends Operator
{
	public String database = null;
	public String host = null;
	public Integer port = null;
	public String rpcServer = null;
	public String table = null;
	public String username = null;
	public String password = null;
	public String selectmodelbycolumn = null;
	public String selectmodelbyvalue = null;
	
	public ClassificationOperator()
	{
		super();
		
		type = Type.PROCESSING;
	}
	
	public ClassificationOperator(Operator operator)
	{
		super(operator);
	}
	
	public ClassificationOperator copy()
	{
		ClassificationOperator operator = new ClassificationOperator(super.copy());
		
		operator.database = this.database;
		operator.host = this.host;
		operator.port = this.port.intValue();
		operator.rpcServer = this.rpcServer;
		operator.table = this.table;
		operator.username = this.username;
		operator.password = this.password;
		operator.selectmodelbycolumn = this.selectmodelbycolumn;
		operator.selectmodelbyvalue = this.selectmodelbyvalue;
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.database, this.host, this.port, this.rpcServer, this.table, this.username, this.password, this.selectmodelbycolumn, this.selectmodelbyvalue, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = ACTIVITYCLASSIFY\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tdatabase='%s',\r\n"
			+ "\t\thost='%s',\r\n"
			+ "\t\tport='%d',\r\n"
			+ "\t\trpcServer='%s',\r\n"
			+ "\t\ttable='%s',\r\n"
			+ "\t\tusername='%s',\r\n"
			+ "\t\tpassword='%s',\r\n"
			+ "\t\tselectmodelbycolumn='%s',\r\n"
			+ "\t\tselectmodelbyvalue='%s'\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}