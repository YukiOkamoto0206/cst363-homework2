package heapdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Table implements ITable {

    private List<Tuple> tuples;
    private Schema schema;

    public Table(Schema schema) {
        this.schema = schema;
        tuples = new ArrayList<>();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }


    @Override
    public int size() {
        return tuples.size();
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean insert(Tuple rec) {
        if (!rec.getSchema().equals(schema)) {
            throw new IllegalStateException("Error: tuple schema does not match table schema.");
        }
        // Added if statement to delete error of last test case
        if (schema.getKey()!=null) {
            for (Tuple tuple : tuples) {
                if (tuple.getKey().equals(rec.getKey())) {
                    return false;
                }
            }
        }
        tuples.add(rec);
        return true;
    }

    @Override
    public boolean delete(Object key) {
        if (schema.getKey() == null) {
            throw new IllegalStateException("Error: table does not have a primary key.  Can not delete.");
        }
        for (Tuple tuple : tuples) {
            if (tuple.getKey().equals(key)) {
                tuples.remove(tuple);
                System.out.println("delete " + tuple.get(0) + ":true");
                return true;
            }
        }
        System.out.println("delete " + key + ":false");
        return false;
    }


    @Override
    public Tuple lookup(Object key) {
        if (schema.getKey() == null) {
            throw new IllegalStateException("Error: table does not have a primary key.  Can not lookup by key.");
        }

        for (Tuple tuple : tuples) {
            if (tuple.getKey().equals(key)) {
                System.out.println(tuple);
            }
        }
        return null;
    }

    @Override
    // using inheritence (
    public ITable lookup(String colname, Object value) {
        if (schema.getColumnIndex(colname) < 0) {
            throw new IllegalStateException("Error: table does not contain column " + colname);
        }
        // parant class  child class
        ITable t = new Table(schema);
        for (Tuple tuple : tuples) {
            if (tuple.get(colname).equals(value)) {
                t.insert(tuple);
                System.out.println(tuple);
            }
        }
        if (t.size() == 0) {
            System.out.println("Empty Table");
        }
        return t;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new TIterator(tuples.iterator());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Tuple t: tuples) {
            sb.append(t.toString());
            sb.append("\n");
        }
        return sb.toString();
//        if (tuples.isEmpty()) {
//            return "Empty Table";
//        } else {
//            StringBuilder sb = new StringBuilder();
//            for (Tuple t : this) {
//                sb.append(t.toString());
//                sb.append("\n");
//            }
//            return sb.toString();
//        }
    }

    /*
     * An iterator that returns a copy of each tuple in
     * the table.
     */
    public static class TIterator implements Iterator<Tuple> {

        private Iterator<Tuple> it;

        public TIterator(Iterator<Tuple> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Tuple next() {
            return new Tuple(it.next());
        }
    }
}
