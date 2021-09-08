package percom.graph.operator;

import java.util.List;

import percom.graph.operator.meta.Operator;

public class ProjectOperator extends Operator
{
	public List<String> attributes = null;
	
	@Override
	public String toString()
	{
		String attributesString = "";
		
		String currentAttribute = null;
		
		for(int index = 0; index < this.attributes.size(); index++)
		{
			currentAttribute = this.attributes.get(index);
			
			attributesString += String.format(QUERY_ATTRIBUTE, currentAttribute, index==this.attributes.size()?"":",");
		}
		
		return String.format(QUERY, this.outputName, attributesString, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = PROJECT\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tattributes =\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t]\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
	
	private static final String QUERY_ATTRIBUTE = 
			  "\t\t\t%s%s\r\n";
}