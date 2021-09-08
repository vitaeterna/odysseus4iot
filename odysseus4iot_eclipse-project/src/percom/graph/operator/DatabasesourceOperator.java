package percom.graph.operator;

import percom.graph.operator.meta.Column;
import percom.graph.operator.meta.Operator;
import percom.graph.operator.meta.Schema;

public class DatabasesourceOperator extends Operator
{
	public String table = null;
	public String jdbc = null;
	public String user = null;
	public String password = null;
	public Schema attributes = null;
	public Integer waiteach = null;
	
	@Override
	public String toString()
	{
		String attributesString = "";
		
		Column currentColumn = null;
		
		for(int index = 0; index < this.attributes.columns.size(); index++)
		{
			currentColumn = this.attributes.columns.get(index);
			
			attributesString += String.format(QUERY_ATTRIBUTE, currentColumn.name, currentColumn.type.getSimpleName(), index==this.attributes.columns.size()?"":",");
		}
		
		return String.format(QUERY, this.outputName, this.table, this.jdbc, this.user, this.password, attributesString, this.waiteach);
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
	
	private static final String QUERY_ATTRIBUTE = 
			  "\t\t\t['%s','%s']%s\r\n";
}