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

	public static void main(String[] args) {
		Schema schema = new Schema();
		schema.addKeyIntType("ID");   // primary key ID int
		schema.addVarCharType("name");
		schema.addVarCharType("dept_name");
		schema.addIntType("salary");
		Tuple[] tuples = new Tuple[] {
				new Tuple(schema, 22222, "Einstein",    "Physics", 95000),
				new Tuple(schema, 12121, "Wu",          "Finance", 90000),
				new Tuple(schema, 32343, "El Said" ,    "History", 60000),
				new Tuple(schema, 45565, "Katz",        "Comp. Sci.", 75000),
				new Tuple(schema, 98345, "Kim",         "Elec. Eng.", 80000),
				new Tuple(schema, 10101, "Srinivasan" , "Comp. Sci.", 65000),
				new Tuple(schema, 76766, "Crick" ,      "Biology", 72000),
		};
		Table table = new Table(schema);
		for (int i = 0; i < tuples.length; i++) {
			table.insert(tuples[i]);
		}
		System.out.println(table.toString());

		Tuple oldTup = new Tuple(schema, 22222, "Einstein",    "Physics", 95000 );
		table.insert(oldTup);
		System.out.println(table.toString());
//		Tuple newTup = new Tuple(schema, 11111, "Molina",      "Music",   70000 );

		System.out.println(table.lookup(22222));
		System.out.println(table.lookup(11111));

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
		if (! rec.getSchema().equals(schema)) {
			throw new IllegalStateException("Error: tuple schema does not match table schema.");
		}
		for (Tuple tuple: tuples) {
			if (tuple.getKey().equals(rec.getKey())) {
				return false;
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
		for (Tuple tuple: tuples) {
			if (tuple.getKey().equals(key)) {
				tuples.remove(tuple);
				return true;
			}
		}
		return false;
	}


	@Override
	public Tuple lookup(Object key) {
		if (schema.getKey() == null) {
			throw new IllegalStateException("Error: table does not have a primary key.  Can not lookup by key.");
		}

		for (Tuple tuple: tuples) {
			if (tuple.getKey().equals(key)) {
				return tuple;
			}
		}
		return null;
	}

	@Override
	public ITable lookup(String colname, Object value) {
		if (schema.getColumnIndex(colname) < 0) {
			throw new IllegalStateException("Error: table does not contain column "+colname);
		}

		// TODO implement this method
		
		throw new  UnsupportedOperationException();

	}

	@Override
	public Iterator<Tuple> iterator() {
		return new TIterator(tuples.iterator());
	}
	
	@Override
	public String toString() {
		
		// TODO implement this method
		if (tuples.isEmpty()) {
			return "Empty Table";
		} else {
			StringBuilder sb = new StringBuilder();
			for (Tuple t : this) {
				sb.append(t.toString());
				sb.append("\n");
			}
			return sb.toString();
		}
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
