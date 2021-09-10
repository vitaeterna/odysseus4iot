package odysseus4iot.main;

import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.gen.OperatorGraphGenerator;
import odysseus4iot.model.Model;
import odysseus4iot.model.PostgresImport;

/*
 * Operator Placement Query Optimization
 * 
 * using JGraphT (https://github.com/jgrapht/jgrapht)
 * 
 * Graph Interface implementation choice:
 * directed       = true
 * self-loops     = false
 * multiple edges = false
 * weighted       = true
 * => SimpleDirectedWeightedGraph/DefaultWeightedEdge
 * 
 * An operator graph is a DAG (directed acyclic graph)
 * 
 * Input:      Models/Nodes
 * Processing: 1. The logical operator graph for models is created
 *             2. The physical node graph is created
 *             3. All permutations of operator assignments to physical nodes are calculated
 *             4. The result is filtered regarding constraints and rated regarding a cost model
 *                4.1. All operators and data flow edges need to be assigned
 *                4.2. The order of operators need to be the same as before
 *                4.3. Cost model regarding network utilization and latency?
 *             5. Best placement is chosen
 *             6. Queries are created for that placement strategy
 * Output:     Queries for each single node
 */
public class Main
{
	public static void main(String[] args)
	{
		//0 - Load property file
		InputStream input = null;
		
		try
		{
			input = new FileInputStream("./config.properties");
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			System.exit(0);
		}

        Properties properties = new Properties();

        try
        {
			properties.load(input);
		}
        catch(IOException e)
        {
			e.printStackTrace();
			System.exit(0);
		}
        
        properties.getProperty("db.url");
		
		//1 - Input to System (sensors/nodes/labels)
		List<String> sensors = new ArrayList<>();
		sensors.add("sensor_data_19");
		
		List<String> nodes = new ArrayList<>();
		
		List<String> labels = new ArrayList<>();
		
		//1 - Retrieving Model Information from Model Management System
		//PostgresImport.url = "jdbc:postgresql://141.13.162.179:5432/procdb";
		//PostgresImport.user = "script";
		//PostgresImport.password = "pAhXHnnFf6jgxO85";
		PostgresImport.url = "jdbc:postgresql://localhost:5432/CattleDB";
		PostgresImport.user = "postgres";
		PostgresImport.password = "postgres";

		List<Model> models = PostgresImport.importFromDB();
		
		//2 - Generating Logical Operator Graphs
		//Here the merging of streams is not considered at first!
		List<Graph> graphs = new ArrayList<>();
		
		Model currentModel = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentModel = models.get(index);
			
			System.out.println(currentModel+"\r\n");
			
			graphs.add(OperatorGraphGenerator.generateOperatorGraph(sensors, currentModel));
		}
	}
}