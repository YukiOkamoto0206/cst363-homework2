package query;

import heapdb.Tuple;

/**
 * A condition of the form colName = value.
 * 
 * @author Glenn
 *
 */

public class EqCondition extends Condition {
	
	private String colName;
	private Object value;
	
	public EqCondition(String colName, Object value) {
		this.colName = colName;
		this.value = value;
	}
	
	public String getColumnName() {
		// TODO replace with your code.
		throw new  UnsupportedOperationException();
	}
	
	public Object getValue() {
		// TODO replace with your code.
		throw new  UnsupportedOperationException();
	}
	
	
	@Override
	public Boolean eval(Tuple tuple) {
		// TODO replace with your code.
		throw new  UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return colName+" = "+value;
	}
}
