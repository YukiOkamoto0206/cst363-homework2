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
		Schema schema;
		if (colNames != null) {
			schema=table.getSchema().project(colNames);
			ITable result_table = new Table(schema);
			for (Tuple tuple : table) {
				if (cond.eval(tuple)) {
					// Need to add project(schema) for passing the last test case
					result_table.insert(tuple.project(schema));
				}
			}
			return result_table;
		} else {
			schema=table.getSchema();
			ITable result_table = new Table(schema);
			for (Tuple tuple : table) {
				if (cond.eval(tuple)) {
					// if you implemented insert(tuple.project(schema)), you will get error at first two test cases
					result_table.insert(tuple);
				}
			}
			return result_table;
		}
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
