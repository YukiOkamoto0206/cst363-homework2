package query;


import heapdb.ITable;
import heapdb.Schema;
import heapdb.Table;
import heapdb.Tuple;

/**
 * A simple select query of the form:
 * select column, column, . . . from table where condition
 * 
 * @author Glenn
 *
 */

public class SelectQuery  {
	
	private Condition cond;
	private String[] colNames;	   // a value of null means return all columns of the table
	
	/**
	 * A query that contains both a where condition and a projection of columns
	 * @param colNames are the columns to return
	 * @param cond is the where clause
	 */
	public SelectQuery(String[] colNames, Condition cond) {
		this.colNames = colNames;
		this.cond = cond;
	}
	
	/**
	 * A query that contains both a where condition.  All columns 
	 * of the Tuples are returned.
	 * @param cond is the where clause
	 */
	public SelectQuery(Condition cond) {
		this(null, cond);
	}
	
	
	public static Table naturalJoin(ITable table1, ITable table2) {
		// TODO replace with your code.
		// Hint: use the Schema naturalJoin method.
		throw new  UnsupportedOperationException();
	}
	
	public ITable eval(ITable table) {
		// TODO replace with your code.
		// 1. create a result Schema if colNames is not null. Use Schema.project method.
		if (colNames != null) {
//			Schema schema = new Schema();
//			schema.project(colNames);
			table.getSchema().project(colNames);
		}
		// 2. create a result Table.
		ITable result_table = new Table(table.getSchema());
		// 3. Iterate over all tuple in table given as argument.
//		for (int i = 0; i< result_table.size(); i++) {
//			result_table.
//		}
		// 4.		for each row,  cond.eval(tuple)
		// 5. 		if false skip to next row.
		// 6.			if ture --> project columns from tuple, using Tuple.project ethod
		// 				insert into result table
		// 7. return result table.
		for (Tuple tuple : table) {
			if (cond.eval(tuple)) {
				result_table.insert(tuple);
			}
		}
		return result_table;
	}

	@Override
	public String toString() {
	    String proj_columns;
	    if (colNames != null) {
	    	proj_columns = String.join(",", colNames);
	    } else {
	    	proj_columns = "*";
	    }
	    return "select " + proj_columns + " where " + cond;
	}

}
