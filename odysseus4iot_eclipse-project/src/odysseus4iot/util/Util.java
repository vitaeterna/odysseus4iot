package odysseus4iot.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.Vertex.Type;
import odysseus4iot.main.Main;

public class Util
{
	public static String readFile(String path, Charset encoding)
    {
        byte[] encoded = null;

        try
        {
            encoded = Files.readAllBytes(Paths.get(path));
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }

        return new String(encoded, encoding);
    }

    public static void writeFile(String path, String content, Charset encoding)
    {
        byte[] encoded = content.getBytes(encoding);

        try
        {
            Files.write(new File(path).toPath(), encoded);
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    
    public static boolean deleteDirectory(File dir)
    {
        if(dir.exists())
        {
            if(dir.isDirectory())
            {
                String[] children = dir.list();

                for(int i = 0; i < children.length; i++)
                {
                    if(!deleteDirectory(new File(dir, children[i])))
                    {
                        return false;
                    }
                }
            }

            return dir.delete();
        }
        else
        {
            return true;
        }
    }

    public static boolean createDirectory(File dir)
    {
        return dir.mkdirs();
    }
	
	public static void validateProperties()
	{
		List<String> requiredProperties = new ArrayList<>();
		requiredProperties.add("input.sensors");
		requiredProperties.add("input.labels");
		requiredProperties.add("sensordb.url");
		requiredProperties.add("sensordb.user");
		requiredProperties.add("sensordb.password");
		requiredProperties.add("modeldb.host");
		requiredProperties.add("modeldb.port");
		requiredProperties.add("modeldb.database");
		requiredProperties.add("modeldb.table");
		requiredProperties.add("modeldb.column");
		requiredProperties.add("modeldb.user");
		requiredProperties.add("modeldb.password");
		
		String currentRequiredProperty = null;
		
		for(int index = 0; index < requiredProperties.size(); index++)
		{
			currentRequiredProperty = requiredProperties.get(index);
			
			if(Main.properties.getProperty(currentRequiredProperty) == null)
			{
				System.err.println("The required property '" + currentRequiredProperty + "' could not be found.");
				
				System.exit(0);
			}
		}
	}
	
    public static String toJson(Object object)
    {
        return (new GsonBuilder().setPrettyPrinting().create()).toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> classT)
    {
        return (new Gson()).fromJson(json, classT);
    }

    public static void charsetUTF8()
    {
        if(!Charset.defaultCharset().name().equals("UTF-8"))
        {
            System.err.print("Default charset is not UTF-8 but " + Charset.defaultCharset().name() + "\n");
            System.err.print("Use VM argument -Dfile.encoding=UTF-8\n");

            System.exit(0);
        }
        else
        {
            System.out.print("Charset: " + Charset.defaultCharset().name() + "\n\n");
        }
    }
	
	public static void exportPQL(String outputFilename, Graph graph)
	{
		StringBuilder stringBuilder = new StringBuilder();
		
		stringBuilder.append("#PARSER PQL\r\n\r\n");
		stringBuilder.append("#REQUIRED de.uniol.inf.is.odysseus.database.feature.feature.group false\r\n\r\n");
		stringBuilder.append("#ADDQUERY\r\n\r\n");
		
		List<Vertex> vertices = graph.getVerticesBreadthFirst();
		
		Vertex currentVertex = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentVertex = vertices.get(index);
			
			stringBuilder.append(currentVertex.toString());
			
			if(index!=vertices.size()-1)
			{
				stringBuilder.append("\r\n\r\n");
			}
		}
		
		Util.writeFile(outputFilename + ".qry", stringBuilder.toString(), Charset.defaultCharset());
		
		System.out.print("Written to " + outputFilename + ".qry\n");
	}
    
    public static void exportDOTPNG(String outputFilename, Graph graph)
    {
        StringBuilder dot = new StringBuilder();

        dot.append("digraph OG\n");
        dot.append("{\n");
        dot.append("    graph [outputorder=edgesfirst, splines=true, dpi=300, fontname=\"Courier New Bold\"];\n");
        dot.append("\n    node [style=filled, fillcolor=white, color=black, fontname=\"Courier New Bold\"];\n");

        Vertex currentVertex = null;
        
        for(int index = 0; index < graph.vertices.size(); index++)
        {
        	currentVertex = graph.vertices.get(index);
        	
        	if(currentVertex.type.equals(Type.SOURCE))
        	{
        		dot.append("    " + currentVertex.id + " [group=g" + currentVertex.group + ", label=\"" + currentVertex.id + "_" + currentVertex.getClass().getSimpleName() + "\", shape=circle, width=1];\n");
        	}
        	else if(currentVertex.type.equals(Type.SINK))
        	{
        		dot.append("    " + currentVertex.id + " [group=g" + currentVertex.group + ", label=\"" + currentVertex.id + "_" + currentVertex.getClass().getSimpleName() + "\", shape=doublecircle, width=1];\n");
        	}
        	else if(currentVertex.type.equals(Type.MERGE))
        	{
        		dot.append("    " + currentVertex.id + " [group=g" + currentVertex.group + ", label=\"" + currentVertex.id + "_" + currentVertex.getClass().getSimpleName() + "\", shape=invtriangle, width=3];\n");
        	}
        	else if(currentVertex.type.equals(Type.BOX))
        	{
        		dot.append("    " + currentVertex.id + " [group=g" + currentVertex.group + ", label=\"" + currentVertex.id + "_" + currentVertex.getClass().getSimpleName() + "\", shape=box, width=2];\n");
        	}
        	else
        	{
        		System.err.println("No routine for Vertex.Type." + currentVertex.type + " implemented.");
        		
        		System.exit(0);
        	}
        }

        dot.append("\n    edge [arrowhead=vee, arrowtail=none, color=black, fontname=\"Courier New Bold\", weight=1];\n");

        Edge currentEdge = null;
        
        for(int index = 0; index < graph.edges.size(); index++)
        {
        	currentEdge = graph.edges.get(index);
        	
        	dot.append("    " + currentEdge.vertex0.id + ":s -> " + currentEdge.vertex1.id + ":n [label=\"" + currentEdge.label + "\"];\n");
        }

        dot.append("}");

        Util.writeFile(outputFilename + ".dot", dot.toString(), Charset.defaultCharset());
        
        System.out.print("Written to " + outputFilename + ".dot\n");
        
        //dot -Tpng outputFilename.dot -o outputFilename.png
        ProcessBuilder builder = new ProcessBuilder("dot", "-Tpng", outputFilename + ".dot", "-o", outputFilename + ".png");
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try
        {
            builder.start().waitFor();

            System.out.print("Written to " + outputFilename + ".png\n");
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();

            System.exit(0);
        }
        catch(IOException e)
        {
            e.printStackTrace();

            System.out.print("\nPlease check whether you have installed a dot renderer like GraphViz (http://www.graphviz.org/) and if the binaries are available via your PATH variable.\n");
            System.out.print("The generated dot file was not rendered to png.\n");
        }
    }
	
    /*
     * Source: http://www.java2s.com/example/java/reflection/find-the-closest-common-superclass-of-multiple-classes.html
     * 
     * Example Call:
     * FindCommonSuperclass(new Class[] {Double.class, Long.class, Integer.class, String.class})
     */
	@SuppressWarnings("rawtypes")
	public static Class FindCommonSuperclass(Class[] cs)
    {
        if (cs.length == 0) {
            return Object.class;
        } else if (cs.length == 1) {
            return cs[0];
        }

        // if any items fail getSuperclass in the passed in array,
        // simply return object.
        boolean isSame = true;
        boolean hasNullSuperclass = false;
        for (Class c : cs) {
            if (c == null)
                throw new NullPointerException();
            if (c != cs[0])
                isSame = false;
            if (c.getSuperclass() == null)
                hasNullSuperclass = true;
        }
        // no need to do further calculations.. all the same
        if (isSame)
            return cs[0];
        // at least one item in the list failed getSuperclass... return object
        if (hasNullSuperclass)
            return Object.class;

        Class c1 = cs[0];
        Class c2 = null;
        HashSet<Class> s1 = new HashSet<>();
        HashSet<Class> s2 = new HashSet<>();

        for (int i = 1; i < cs.length; i++) {
            s1.clear();
            s2.clear();
            c2 = cs[i];

            do {
                s1.add(c1);
                s2.add(c2);
                if (c1 != Object.class) {
                    c1 = c1.getSuperclass();
                }
                if (c2 != Object.class) {
                    c2 = c2.getSuperclass();
                }
            } while (Collections.disjoint(s1, s2));

            s1.retainAll(s2);
            c1 = s1.iterator().next(); // there can only be one
            if (c1 == Object.class)
                break; // no superclass above object
        }
        return c1;
    }
}