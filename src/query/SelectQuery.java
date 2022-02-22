package query;


import heapdb.ITable;
import heapdb.Schema;
import heapdb.Table;
import heapdb.Tuple;

import java.util.ArrayList;

/**
 * A simple select query of the form:
 * select column, column, . . . from table where condition
 *
 * @author Glenn
 */

public class SelectQuery {

    private Condition cond;
    private String[] colNames;       // a value of null means return all columns of the table

    /**
     * A query that contains both a where condition and a projection of columns
     *
     * @param colNames are the columns to return
     * @param cond     is the where clause
     */
    public SelectQuery(String[] colNames, Condition cond) {
        this.colNames = colNames;
        this.cond = cond;
    }

    /**
     * A query that contains both a where condition.  All columns
     * of the Tuples are returned.
     *
     * @param cond is the where clause
     */
    public SelectQuery(Condition cond) {
        this(null, cond);
    }


    public static Table naturalJoin(ITable table1, ITable table2) {
        Schema resultSchema = table1.getSchema().naturaljoin(table2.getSchema());
        Table result = new Table(resultSchema);

        Schema schema1 = table1.getSchema();
        Schema schema2 = table2.getSchema();

        // duplicate method
        ArrayList<String> schemaList = new ArrayList<>();
        for (int i = 0; i < schema1.size(); i++) {
            for (int j = 0; j < schema2.size(); j++) {
                if (schema1.getName(i).equals(schema2.getName(j))) {
                    if (!schemaList.contains(table1.getSchema().getName(i))) {
                        schemaList.add(schema1.getName(i)); // list of duplicated cols
                    }
                }
            }
        }

        // nested loop join
        for (Tuple t1 : table1) {
            for (Tuple t2 : table2) {

                boolean isAdd = true;

                // schemaList.size() -> 2
                for (int i = 0; i < schemaList.size(); i++) {
                    // schemaList.get(i) -> B or D
                    if (!(t1.get(schemaList.get(i)).equals(t2.get(schemaList.get(i))))) {
                        // if it happens once, no add
                        isAdd = false;
                        break;
                    }
                }
                if (isAdd) {
                    Tuple r = Tuple.joinTuple(resultSchema, t1, t2);
                    result.insert(r);
                }
            }
        }
        return result;
    }

    public ITable eval(ITable table) {
        // TODO replace with your code.
        Schema schema;
        if (colNames != null) {
            schema = table.getSchema().project(colNames);
            ITable result_table = new Table(schema);
            for (Tuple tuple : table) {
                if (cond.eval(tuple)) {
                    // Need to add project(schema) for passing the last test case
                    result_table.insert(tuple.project(schema));
                }
            }
            return result_table;
        } else {
            schema = table.getSchema();
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
