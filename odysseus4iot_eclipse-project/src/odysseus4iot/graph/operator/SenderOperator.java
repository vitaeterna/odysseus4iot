package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

/**
 * POJO representing the Odysseus Operator SENDER.
 * 
 * @author Michael Sünkel
 */
public class SenderOperator extends Operator
{
	private static Integer senderCount = 0;
	
	public static Integer getCurrentSenderCount()
	{
		return senderCount;
	}
	
	public static Integer getNextSenderCount()
	{
		return ++senderCount;
	}
	
	public static void resetSenderCount()
	{
		senderCount = 0;
	}
	
	public String host = null;
	public Integer port = null;
	
	public SenderOperator()
	{
		super();
		
		type = Type.SINK;
	}
	
	public SenderOperator(Operator operator)
	{
		super(operator);
	}
	
	public SenderOperator copy()
	{
		SenderOperator operator = new SenderOperator(super.copy());
		
		operator.host = this.host;
		operator.port = this.port.intValue();
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.outputName, this.host, this.port, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = SENDER\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tsink='%s',\r\n"
			+ "\t\twrapper='GenericPush',\r\n"
			+ "\t\ttransport='TCPClient',\r\n"
			+ "\t\tprotocol='Odysseus',\r\n"
			+ "\t\tdatahandler='Tuple',\r\n"
			+ "\t\twritemetadata=true,\r\n"
			+ "\t\toptions=\r\n"
			+ "\t\t[\r\n"
			+ "\t\t\t['host','%s'],\r\n"
			+ "\t\t\t['port',%d],\r\n"
			+ "\t\t\t['byteorder','Little_Endian'],\r\n"
			+ "\t\t\t['objectsize','10240']\r\n"
			+ "\t\t]\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}