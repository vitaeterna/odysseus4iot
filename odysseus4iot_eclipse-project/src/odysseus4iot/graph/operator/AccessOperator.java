package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.Schema;

/**
 * POJO representing the Odysseus Operator ACCESS.
 * 
 * @author Michael Sünkel
 */
public class AccessOperator extends Operator
{
	private static Integer accessCount = 0;
	
	public static Integer getCurrentAccessCount()
	{
		return accessCount;
	}
	
	public static Integer getNextAccessCount()
	{
		return ++accessCount;
	}
	
	public static void resetAccessCount()
	{
		accessCount = 0;
	}
	
	public String host = null;
	public Integer port = null;
	public Schema attributes = null;
	
	public AccessOperator()
	{
		super();
		
		type = Type.SOURCE;
	}
	
	public AccessOperator(Operator operator)
	{
		super(operator);
	}
	
	public AccessOperator copy()
	{
		AccessOperator operator = new AccessOperator(super.copy());
		
		operator.host = this.host;
		operator.port = this.port.intValue();
		operator.attributes = this.attributes.copy();
		
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
		
		return String.format(QUERY, this.outputName, this.outputName, this.host, this.port, attributesString);
	}
	
	public static final String QUERY = 
			  "%s = ACCESS\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tsource='%s',\r\n"
			+ "\t\twrapper='GenericPush',\r\n"
			+ "\t\ttransport='TCPServer',\r\n"
			+ "\t\tprotocol='Odysseus',\r\n"
			+ "\t\tdatahandler='Tuple',\r\n"
			+ "\t\treadmetadata=true,\r\n"
			+ "\t\toptions=\r\n"
			+ "\t\t[\r\n"
			+ "\t\t\t['host','%s'],\r\n"
			+ "\t\t\t['port',%d],\r\n"
			+ "\t\t\t['byteorder','Little_Endian'],\r\n"
			+ "\t\t\t['objectsize','10240']\r\n"
			+ "\t\t],\r\n"
			+ "\t\tschema=\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t]\r\n"
			+ "\t}\r\n"
			+ ")";
	
	private static final String QUERY_ATTRIBUTE = 
			  "\t\t\t['%s','%s']%s\r\n";
}