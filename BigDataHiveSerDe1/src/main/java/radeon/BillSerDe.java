package radeon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BillSerDe implements SerDe {
	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
	private List<String> colNames;
	private List<Object> row = new ArrayList<Object>();


	public Object deserialize(Writable blob) throws SerDeException {
		Text rowText = (Text)blob;
		
		row.clear();
		
		String rowString = rowText.toString();
		String[] fields = rowString.split(",");
		String[] products = Arrays.copyOfRange(fields, 1, fields.length);
		
		/*for (String field : fields) {
			System.out.print(field + ",");
		}
		System.out.println();
		
		for (String product : products) {
			System.out.print(product + ",");
		}
		System.out.println();*/
		
		List<String> prodList;
		
		for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
			if (fieldName.equals("my_date")) {
				row.add(fields[0]);
			} else if (fieldName.equals("products")) { //products
				prodList = new ArrayList<String>();
				for (String product : products) {
					prodList.add(product);
				}
				row.add(prodList.toArray());
			}
			
		}
		return row;
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		// Get a list of the table's column names.
		String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));

		// Get a list of TypeInfos for the columns. This list lines up with
		// the list of column names.
		String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
		List<TypeInfo> colTypes =
				TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

		rowTypeInfo =
				(StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI =
				TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

	}

	public Class<? extends Writable> getSerializedClass() {
		// Not-implemented
		return null;
	}

	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		// Not-implemented
		return null;
	}

}
