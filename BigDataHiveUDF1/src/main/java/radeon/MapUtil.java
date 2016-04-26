package radeon;

import java.util.*;

public class MapUtil
{
	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map) { //default
		return MapUtil.sortByValue(map, "asc");
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map, final String order )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{	
				if (order.equals("desc"))
					return (o2.getValue()).compareTo( o1.getValue() );
				else 
					return (o1.getValue()).compareTo( o2.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
}