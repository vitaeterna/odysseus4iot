package main;

import java.util.HashSet;
import java.util.Locale;
import java.util.Collections;

public class Main
{
	public static void main(String[] args)
	{
		System.out.println(Locale.getDefault());
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