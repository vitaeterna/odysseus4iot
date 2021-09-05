package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class ClassificationOperator extends Operator
{
	public static final String QUERY = 
			  "prediction = ACTIVITYPREDICT\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tdatabase='procdb',\r\n"
			+ "\t\thost='141.13.162.179',\r\n"
			+ "\t\tport='5432',\r\n"
			+ "\t\trpcServer='localhost:9000',\r\n"
			+ "\t\ttable='experiment_result',\r\n"
			+ "\t\tusername=<username>,\r\n"
			+ "\t\tpassword=<password>,\r\n"
			+ "\t\tselectmodelbycolumn='model_title',\r\n"
			+ "\t\tselectmodelbyvalue='3Cattle1FarmRF_Lying'\r\n"
			+ "\t},\r\n"
			+ "\taggFeaturesRate\r\n"
			+ ")";
}
