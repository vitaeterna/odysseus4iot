package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.Schema;
import odysseus4iot.main.Main;

public class DatabasesourceOperator extends Operator
{
	public String table = null;
	public String jdbc = null;
	public String user = null;
	public String password = null;
	public Schema attributes = null;
	public Integer waiteach = null;
	
	public DatabasesourceOperator()
	{
		super();
		
		type = Type.SOURCE;
	}
	
	public DatabasesourceOperator(Operator operator)
	{
		super(operator);
	}
	
	public DatabasesourceOperator copy()
	{
		DatabasesourceOperator operator = new DatabasesourceOperator(super.copy());
		
		operator.table = this.table;
		operator.jdbc = this.jdbc;
		operator.user = this.user;
		operator.password = this.password;
		operator.attributes = this.attributes.copy();
		operator.waiteach = this.waiteach.intValue();
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		String attributesString = "";
		
		Column currentColumn = null;
		
		for(int index = 0; index < this.attributes.columns.size(); index++)
		{
			currentColumn = this.attributes.columns.get(index);
			
			attributesString += String.format(QUERY_ATTRIBUTE, currentColumn.name, currentColumn.type.getSimpleName(), index==this.attributes.columns.size()-1?"":",");
		}
		
		return String.format(QUERY, this.outputName, this.table, this.jdbc, this.user, this.password, this.id, attributesString, (int)(this.waiteach/Main.evaluationSpeedupFactor));
	}
	
	private static final String QUERY = 
			  "%s = DATABASESOURCE\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\ttable='%s',\r\n"
			+ "\t\tjdbc='%s',\r\n"
			+ "\t\tuser='%s',\r\n"
			+ "\t\tpassword='%s',\r\n"
			+ "\t\tconnection='connection%d',\r\n"
			+ "\t\tattributes=\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t],\r\n"
			+ "\t\twaiteach=%d\r\n"
			+ "\t}\r\n"
			+ ")";
	
	private static final String QUERY_ATTRIBUTE = 
			  "\t\t\t['%s','%s']%s\r\n";
}