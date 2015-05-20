package fct.thesis.databaseNMSI;

import java.util.*;

/**
 * Created by gomes on 08/04/15.
 */
public class ListIndexable<T> {

    Long actualVersion = -1L;
    HashMap<Long, Integer> versionsToPositions = new HashMap<Long, Integer>();
    ArrayList<T> list = new ArrayList<T>(1000);

    public int indexForVersion(long version){
        assert (actualVersion>= version);

        if (actualVersion==version)
            return list.size();

        return versionsToPositions.get(version+1);
    }

    public void putObject(Long version, T object){
        if (version > actualVersion){
            versionsToPositions.put(version, list.size());
            actualVersion = version;
        }
        list.add(object);
    }

    public void putObject(T object){
        putObject(actualVersion, object);
    }

    public ListIterator<T> objectLessThan(Long version){
        return list.listIterator(indexForVersion(version));
    }

    public static void main(String[] args) {

        ListIndexable<String> hr = new ListIndexable<String>();

        hr.putObject(0L,"a");
        hr.putObject("b");
        hr.putObject("c");

        hr.putObject(1L,"d");
        hr.putObject("e");

        hr.putObject(2L,"f");

        hr.putObject(3L, "g");
        hr.putObject("h");
        hr.putObject("l");

        System.out.print("Version 0 indexForVersion=" + hr.indexForVersion(0L) + " :");
        print(hr.objectLessThan(0L));

        System.out.print("Version 1 indexForVersion="+hr.indexForVersion(1L)+" :");
        print(hr.objectLessThan(1L));

        System.out.print("Version 2 indexForVersion="+hr.indexForVersion(1L)+" :");
        print(hr.objectLessThan(2L));

        System.out.print("Version 3 indexForVersion="+hr.indexForVersion(3L)+" :");
        print(hr.objectLessThan(3L));

        /* Console:
            Version 0 indexForVersion=3 :c, b, a,
            Version 1 indexForVersion=5 :e, d, c, b, a,
            Version 2 indexForVersion=5 :f, e, d, c, b, a,
            Version 3 indexForVersion=9 :l, h, g, f, e, d, c, b, a,
         */

    }

    public static void print(ListIterator<String> it){
        while (it.hasPrevious()){
            System.out.print(it.previous() + ", ");
        }
        System.out.println();
    }

}
