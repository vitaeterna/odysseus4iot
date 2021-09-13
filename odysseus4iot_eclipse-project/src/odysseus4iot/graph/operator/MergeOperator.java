package odysseus4iot.graph.operator;

import java.util.List;

import odysseus4iot.graph.operator.meta.Operator;

public class MergeOperator extends Operator
{
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