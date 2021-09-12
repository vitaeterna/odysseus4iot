package odysseus4iot.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import odysseus4iot.main.Main;

public class Util
{
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