package jOperatorPlacement.graphGenerator;

import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import jOperatorPlacement.queryGenerator.QueryGenerator;

public class OperatorGraphGenerator
{
	public static Graph<Operator, DefaultWeightedEdge> optimizedGraph()
	{
		Graph<Operator, DefaultWeightedEdge> operatorGraph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		
		Operator operator0 = new DatabasesourceOperator();
		Operator operator1 = new MapOperator();
		Operator operator2 = new TimewindowOperator();
		Operator operator3 = new AggregateOperator();
		Operator operator4 = new ClassificationOperator();
		Operator operator5 = new OutlierRemovingOperator();
		Operator operator6 = new ChangedetectOperator();
		
		operatorGraph.addVertex(operator0);
		operatorGraph.addVertex(operator1);
		operatorGraph.addVertex(operator2);
		operatorGraph.addVertex(operator3);
		operatorGraph.addVertex(operator4);
		operatorGraph.addVertex(operator5);
		operatorGraph.addVertex(operator6);
		
		DefaultWeightedEdge dwe1 = operatorGraph.addEdge(operator0, operator1);
		operatorGraph.setEdgeWeight(dwe1, 0);
		
		DefaultWeightedEdge dwe2 = operatorGraph.addEdge(operator1, operator2);
		operatorGraph.setEdgeWeight(dwe2, 0);
		
		DefaultWeightedEdge dwe3 = operatorGraph.addEdge(operator2, operator3);
		operatorGraph.setEdgeWeight(dwe3, 0);
		
		DefaultWeightedEdge dwe4 = operatorGraph.addEdge(operator3, operator4);
		operatorGraph.setEdgeWeight(dwe4, 0);
		
		DefaultWeightedEdge dwe5 = operatorGraph.addEdge(operator4, operator5);
		operatorGraph.setEdgeWeight(dwe5, 0);
		
		DefaultWeightedEdge dwe6 = operatorGraph.addEdge(operator5, operator6);
		operatorGraph.setEdgeWeight(dwe6, 0);
		
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_DATABASESOURCE);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_MAP);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_TIMEWINDOW);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_AGGREGATE);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_ACTIVITYPREDICT);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_OUTLIERREMOVAL);
		System.out.print("\r\n#####\r\n");
		System.out.print(QueryGenerator.QUERY_CHANGEDETECT);
		System.out.print("\r\n#####\r\n");
		
        DOTExporter<Operator, DefaultWeightedEdge> exporter = new DOTExporter<>();
        exporter.setVertexAttributeProvider
        (
        	(v) ->
        	{
                Map<String, Attribute> map = new LinkedHashMap<>();
                map.put("label", DefaultAttribute.createAttribute(v.toString()));
                return map;
        	}
        );
        Writer writer = new StringWriter();
        exporter.exportGraph(operatorGraph, writer);
        System.out.println(writer.toString());
        
        return operatorGraph;
	}
}