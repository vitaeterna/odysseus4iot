package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class DatarateOperator extends Operator
{
	private static Integer datarateCount = 0;
	
	public static Integer getCurrentDatarateCount()
	{
		return datarateCount;
	}
	
	public static Integer getNextDatarateCount()
	{
		return ++datarateCount;
	}
	
	public static void resetDatarateCount()
	{
		datarateCount = 0;
	}
	
	public String key = null;
	
	public DatarateOperator()
	{
		super();
		
		type = Type.BENCHMARK;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.outputName, this.key, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = DATARATE\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tupdaterate=0,\r\n"
			+ "\t\ttype='BYTE',\r\n"
			+ "\t\tincludemetadata=false,\r\n"
			+ "\t\tkey='%s'\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
}