/*
 * Based from https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/collect/MapRemoveKeysUDF.java
 */

package radeon;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

public class BestFiveMapUDF extends GenericUDF {
	private MapObjectInspector mapInspector; //inspector della mappa di input
    private StandardMapObjectInspector retValInspector; //inspector della mappa di output
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		Map<String, Integer> mapInput = (Map<String, Integer>)this.mapInspector.getMap(args[0].get());
		//Map<String, Integer> mapOutput = (Map<String, Integer>)this.retValInspector.create();
		Map<String, Integer> mapOutput = new TreeMap<String, Integer>();
		
		if (mapInput == null)
			return null;
		
		mapInput = MapUtil.sortByValue(mapInput, "desc");
		
		//mapOutput.putAll(mapInput);
		
		return mapInput;
		
	}

	@Override
	public String getDisplayString(String[] children) {
		return "best_five_map()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentLengthException("best_five_map only takes 1 argument: Map<K,V>");
		}
		
		ObjectInspector first = args[0];
        if (first.getCategory() == Category.MAP) {
            mapInspector = (MapObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting a map as first argument ");
        }

        retValInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
	}

}
