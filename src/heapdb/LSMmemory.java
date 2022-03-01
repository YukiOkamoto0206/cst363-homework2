package heapdb;


import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class LSMmemory implements ITable {

    private Schema schema;
    private TreeMap<Object, Tuple> level0;

    /**
     * create a new LSM table
     */
    public LSMmemory(String filename, Schema schema) {
        this.schema = schema;
        level0 = new TreeMap<>();
    }

    public LSMmemory(String filename) {
        this.level0 = new TreeMap<>();
    }

    @Override
    public void close() {
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("LSM Table does not support size().");
    }

    @Override
    public boolean insert(Tuple rec) {
        // DO not check for duplicate key.
        // Allow a duplicate key to replace current entry in the TreeMap.
        // use Tuple copy constructor to make a copy of rec.  put copy into TreeMap.
        Tuple t = new Tuple(rec); // copy constructor.
        // Why a copy?
        // 	prevent user from modifying tuple after it has
        //   been added into the tree and constraints (if any) have been checked.
        level0.put(t.getKey(), t); // up-sert update+insert
        return true;

    }

    @Override
    public boolean delete(Object key) {
        Tuple t = level0.remove(key);
        // if t == null, them key did not exist.
        // t contains the removed tuple
        return true;
    }

    @Override
    public Tuple lookup(Object key) {
        if (level0.get(key) != null) {
            return level0.get(key);
        }
        return null;
    }

    @Override
    public ITable lookup(String colname, Object value) {
        return null;
    }

    @Override
    public String toString() {
        if (schema.size() ==0) {
            return "Empty Table";
        }else {
            StringBuilder sb = new StringBuilder();
            for (Tuple t : this) {
                sb.append(t.toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new LSMmemoryIterator();
    }


    public class LSMmemoryIterator implements Iterator<Tuple> {
        Iterator<Map.Entry<Object, Tuple>> it0 = level0.entrySet().iterator();

        @Override
        public boolean hasNext() {
            return it0.hasNext();
        }

        @Override
        public Tuple next() {
            return it0.next().getValue();
        }
    }
}
