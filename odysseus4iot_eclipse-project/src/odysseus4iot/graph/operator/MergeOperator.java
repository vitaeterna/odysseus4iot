package odysseus4iot.graph.operator;

import java.util.ArrayList;
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
	
	public MergeOperator(Operator operator)
	{
		super(operator);
	}
	
	public MergeOperator copy()
	{
		MergeOperator operator = new MergeOperator(super.copy());
		
		operator.inputStreams = new ArrayList<>(this.inputStreams);
		
		return operator;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%s%d_%s%s", this.inputStreams!=null?this.inputStreams.size()+"\n":"", this.id, this.getClass().getSimpleName(), this.outputName!=null?"\n"+this.outputName:"");
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