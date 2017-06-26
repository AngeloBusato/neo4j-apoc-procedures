package apoc.atomic;

import apoc.atomic.util.AtomicUtils;
import apoc.util.ArrayBackedList;
import apoc.util.MapUtil;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author AgileLARUS
 *
 * @since 20-06-17
 */
public class Atomic {

	@Context public GraphDatabaseAPI db;

	/**
	 * increment a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.add(node/relatonship,propertyName,number) Sums the property's value with the 'number' value ")
	public Stream<AtomicResults> add(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number) {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Number oldValue;
		Number newValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = (Number) propertyContainer.getProperty(property);
			Lock lock = tx.acquireReadLock(propertyContainer);
			newValue = AtomicUtils.sum(oldValue, number);
			propertyContainer.setProperty(property, newValue);
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue, newValue));
	}

	/**
	 * decrement a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.subtract(node/relatonship,propertyName,number) Subtracts the 'number' value to the property's value")
	public Stream<AtomicResults> subtract(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number){
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Number oldValue;
		Number newValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = (Number) propertyContainer.getProperty(property);
			Lock lock = tx.acquireReadLock(propertyContainer);
			newValue = AtomicUtils.sub(oldValue, number);
			propertyContainer.setProperty(property, newValue);
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,newValue));
	}

	/**
	 * concat a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.concat(node/relatonship,propertyName,string) Concats the property's value with the 'string' value")
	public Stream<AtomicResults> concat(@Name("container") Object container, @Name("propertyName") String property, @Name("string") String string){
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		String oldValue;
		String newValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = propertyContainer.getProperty(property).toString();
			Lock lock = tx.acquireReadLock(propertyContainer);
			newValue = oldValue.concat(string);
			propertyContainer.setProperty(property, newValue);
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,newValue));
	}

	/**
	 * insert a value into an array property value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.insert(node/relatonship,propertyName,position,value) insert a value into the property's array value at 'position'")
	public Stream<AtomicResults> insert(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position, @Name("value") Object value) throws ClassNotFoundException {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Object oldValue;
		Object newValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = propertyContainer.getProperty(property);
			Lock lock = tx.acquireReadLock(propertyContainer);
			List<Object> values = insertValueIntoArray(oldValue, position, value);
			Class clazz = Class.forName(values.toArray()[0].getClass().getName());
			newValue = Array.newInstance(clazz, values.size());
			try {
				System.arraycopy(values.toArray(), 0, newValue, 0, values.size());
			} catch (Exception e) {
				String message = "Property's array value has type: " + values.toArray()[0].getClass().getName() + ", and your value to insert has type: " + value.getClass().getName();
				throw new ArrayStoreException(message);
			}
			propertyContainer.setProperty(property, newValue);
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,newValue));
	}

	/**
	 * remove a value into an array property value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.remove(node/relatonship,propertyName,position) remove the element at position 'position'")
	public Stream<AtomicResults> remove(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position) throws ClassNotFoundException {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Object oldValue;
		Object newValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = propertyContainer.getProperty(property);
			Lock lock = tx.acquireReadLock(propertyContainer);
			Object[] arrayBackedList = new ArrayBackedList(oldValue).toArray();
			List<Object> value = new ArrayList<>();

			for(int i= 0; i< arrayBackedList.length; i++) {
				if(i != position.intValue()) value.add(arrayBackedList[i]);
			}
			Class clazz = Class.forName(value.toArray()[0].getClass().getName());
			newValue = Array.newInstance(clazz, value.size());
			System.arraycopy(value.toArray(), 0, newValue, 0, value.size());
			propertyContainer.setProperty(property, newValue);
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,newValue));
	}

	/**
	 * update the property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.update(node/relatonship,propertyName,updateOperation) update a property's value with a cypher operation (ex. \"n.prop1+n.prop2\")")
	public Stream<AtomicResults> update(@Name("container") Object container, @Name("propertyName") String property, @Name("operation") String operation){
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Object oldValue;
		try(Transaction tx = db.beginTx()){
			propertyContainer = (PropertyContainer) container;
			oldValue = propertyContainer.getProperty(property);
			Lock lock = tx.acquireReadLock(propertyContainer);
			db.execute("UNWIND {container} as n with n set n." + property + "=" + operation + ";", MapUtil.map("container", propertyContainer));
			tx.success();
			lock.release();
		}
		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,propertyContainer.getProperty(property)));
	}

	private List<Object> insertValueIntoArray(Object oldValue, Long position, Object value) {
		List<Object> values = new ArrayList<>();
		if (oldValue.getClass().isArray())
			values.addAll(new ArrayBackedList(oldValue));
		else
			values.add(oldValue);
		if(position > values.size())
			values.add(value);
		else
			values.add(position.intValue(), value);
		return values;
	}

	private void checkIsPropertyContainer(Object container) {
		if(!(container instanceof PropertyContainer))  throw new RuntimeException("You Must pass Node or Relationship");
	}

	public class AtomicResults{
		public Object container;
		public String property;
		public Object oldValue;
		public Object newValue;

		public AtomicResults(Object container, String property, Object oldValue, Object newValue) {
			this.container = container;
			this.property = property;
			this.oldValue = oldValue;
			this.newValue = newValue;
		}
	}

}
