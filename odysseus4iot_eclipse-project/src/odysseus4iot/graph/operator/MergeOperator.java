package odysseus4iot.graph.operator;

import java.util.List;

import odysseus4iot.graph.operator.meta.Operator;

public class MergeOperator extends Operator
{
	private static Integer mergeCount = 0;
	
	public static Integer getCurrentMergeCount()
	{
		return mergeCount;
	}
	
	public static Integer getNextMergeCount()
	{
		return ++mergeCount;
	}
	
	public static void resetMergeCount()
	{
		mergeCount = 0;
	}
	
	public List<String> inputStreams = null;
	
	public MergeOperator()
	{
		super();
		
		type = Type.MERGE;
	}
	
	@Override
	public String toString()
	{
		String inputStreamsString = "";
		
		String currentInputStream = null;
		
		for(int index = 0; index < this.inputStreams.size(); index++)
		{
			currentInputStream = this.inputStreams.get(index);
			
			inputStreamsString += String.format(QUERY_INPUTSTREAM, currentInputStream, index==this.inputStreams.size()-1?"":",");
		}
		
		return String.format(QUERY, this.outputName, inputStreamsString);
	}
	
	public static final String QUERY = 
			  "%s = MERGE\r\n"
			+ "(\r\n"
			+ "%s"
			+ ")";
	
	private static final String QUERY_INPUTSTREAM = 
			  "\t%s%s\r\n";
}