package apoc.atomic;

import apoc.atomic.util.AtomicUtils;
import apoc.util.ArrayBackedList;
import apoc.util.MapUtil;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.helpers.TransactionTemplate;

/**
 * @author AgileLARUS
 * @since 20-06-17
 */
public class Atomic {

	@Context
	public GraphDatabaseAPI db;

	/**
	 * increment a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.add(node/relatonship,propertyName,number) Sums the property's value with the 'number' value ")
	public Stream<AtomicResults> add(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number, @Name("times") Long times) {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Number newValue = 0;
		propertyContainer = (PropertyContainer) container;

		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
		retry(executionContext, (context) -> {
			AtomicUtils.sum((Number) propertyContainer.getProperty(property), number);
			propertyContainer.setProperty(property, AtomicUtils.sum((Number) propertyContainer.getProperty(property), number));
			return AtomicUtils.sum((Number) propertyContainer.getProperty(property), number);
		}, times);

		return Stream.of(new AtomicResults(propertyContainer,property,(Number) propertyContainer.getProperty(property), newValue));
	}

	/**
	 * decrement a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.subtract(node/relatonship,propertyName,number) Subtracts the 'number' value to the property's value")
	public Stream<AtomicResults> subtract(@Name("container") Object container, @Name("propertyName") String property, @Name("number") Number number, @Name("times") Long times) {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		Number newValue=0;
		propertyContainer = (PropertyContainer) container;

		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
		retry(executionContext, (context) -> {
			AtomicUtils.sub((Number) propertyContainer.getProperty(property), number);
			propertyContainer.setProperty(property, AtomicUtils.sub((Number) propertyContainer.getProperty(property), number));
			return AtomicUtils.sub((Number) propertyContainer.getProperty(property), number);
		}, times);

		return Stream.of(new AtomicResults(propertyContainer, property, (Number) propertyContainer.getProperty(property), newValue));
	}

	/**
	 * concat a property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.concat(node/relatonship,propertyName,string) Concats the property's value with the 'string' value")
	public Stream<AtomicResults> concat(@Name("container") Object container, @Name("propertyName") String property, @Name("string") String string, @Name("times") Long times) {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		final String[] oldValue = new String[1];
		final String[] newValue = new String[1];
		propertyContainer = (PropertyContainer) container;

		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);
		retry(executionContext, (context) -> {
			oldValue[0] = propertyContainer.getProperty(property).toString();
			newValue[0] = oldValue[0].concat(string);
			propertyContainer.setProperty(property, newValue[0]);

			return oldValue[0].concat(string);
		}, times);

		return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
	}

	/**
	 * insert a value into an array property value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.insert(node/relatonship,propertyName,position,value) insert a value into the property's array value at 'position'")
	public Stream<AtomicResults> insert(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position, @Name("value") Object value, @Name("times") Long times) throws ClassNotFoundException {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		final Object[] oldValue = new Object[1];
		final Object[] newValue = new Object[1];

		propertyContainer = (PropertyContainer) container;
		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

		retry(executionContext, (context) -> {
			List<Object> values = insertValueIntoArray(propertyContainer.getProperty(property), position, value);
			Class clazz = null;
			try {
				clazz = Class.forName(values.toArray()[0].getClass().getName());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			newValue[0] = Array.newInstance(clazz, values.size());
			try {
				System.arraycopy(values.toArray(), 0, newValue[0], 0, values.size());
			} catch (Exception e) {
				String message = "Property's array value has type: " + values.toArray()[0].getClass().getName() + ", and your value to insert has type: " + value.getClass().getName();
				throw new ArrayStoreException(message);
			}
			propertyContainer.setProperty(property, newValue[0]);
			return Array.newInstance(clazz, values.size());
		}, times);

		return Stream.of(new AtomicResults(propertyContainer, property, oldValue[0], newValue[0]));
	}

	/**
	 * remove a value into an array property value
	 */
/*    @Procedure(mode = Mode.WRITE)
    @Description("apoc.atomic.remove(node/relatonship,propertyName,position) remove the element at position 'position'")
    public Stream<AtomicResults> remove(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position) throws ClassNotFoundException {
        checkIsPropertyContainer(container);
        PropertyContainer propertyContainer;
        Object oldValue = null;
        Object newValue = null;



        propertyContainer = (PropertyContainer) container;
        final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

        retry(executionContext, (ExecutionContext context) -> {
            //oldValue = propertyContainer.getProperty(property);

            Object[] arrayBackedList = new ArrayBackedList(propertyContainer.getProperty(property)).toArray();
            List<Object> value = new ArrayList<>();

            for (int i = 0; i < arrayBackedList.length; i++) {
                if (i != position.intValue()) value.add(arrayBackedList[i]);
            }
            Class clazz = null;
            try {
                clazz = Class.forName(value.toArray()[0].getClass().getName());
            } catch (ClassNotFoundException e) {
                throw new ClassCastException();
            }
            //newValue = Array.newInstance(clazz, value.size());
            System.arraycopy(value.toArray(), 0, Array.newInstance(clazz, value.size()), 0, value.size());

            propertyContainer.setProperty(property, Array.newInstance(clazz, value.size()));
            return Array.newInstance(clazz, value.size());
        }, 5);

        return Stream.of(new AtomicResults(propertyContainer, property, oldValue, newValue));
    }*/

	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.remove(node/relatonship,propertyName,position) remove the element at position 'position'")
	public Stream<AtomicResults> remove(@Name("container") Object container, @Name("propertyName") String property, @Name("position") Long position, @Name("times") Long times) throws ClassNotFoundException {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer;
		final Object[] oldValue = new Object[1];
		final Object[] newValue = new Object[1];

		propertyContainer = (PropertyContainer) container;
		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

		retry(executionContext, (ExecutionContext context) -> {

			oldValue[0] = propertyContainer.getProperty(property);

			Object[] arrayBackedList = new ArrayBackedList(oldValue[0]).toArray();
			List<Object> value = new ArrayList<>();

			for(int i= 0; i< arrayBackedList.length; i++) {
				if(i != position.intValue()) value.add(arrayBackedList[i]);
			}
			Class clazz = null;
			try {
				clazz = Class.forName(value.toArray()[0].getClass().getName());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			newValue[0] = Array.newInstance(clazz, value.size());
			System.arraycopy(value.toArray(), 0, newValue[0], 0, value.size());
			propertyContainer.setProperty(property, newValue[0]);

			return context.propertyContainer.getProperty(property);

		}, times);


		return Stream.of(new AtomicResults(propertyContainer,property, oldValue[0], propertyContainer.getProperty(property)));
	}

	/**
	 * update the property's value
	 */
	@Procedure(mode = Mode.WRITE)
	@Description("apoc.atomic.update(node/relatonship,propertyName,updateOperation) update a property's value with a cypher operation (ex. \"n.prop1+n.prop2\")")
	public Stream<AtomicResults> update(@Name("container") Object container, @Name("propertyName") String property, @Name("operation") String operation, @Name("times") Long times) throws InterruptedException {
		checkIsPropertyContainer(container);
		PropertyContainer propertyContainer = (PropertyContainer) container;
		Object oldValue = propertyContainer.getProperty(property);

		final ExecutionContext executionContext = new ExecutionContext(db, propertyContainer, property);

		retry(executionContext, (context) -> {
			String statement = "UNWIND {container} as n with n set n." + property + "=" + operation + ";";
			Map<String, Object> properties = MapUtil.map("container", propertyContainer);
			return context.db.execute(statement, properties);
		}, times);

		return Stream.of(new AtomicResults(propertyContainer,property,oldValue,propertyContainer.getProperty(property)));
	}

	private static class ExecutionContext {
		private final GraphDatabaseService db;

		private final PropertyContainer propertyContainer;

		private final String propertyName;

		public ExecutionContext(GraphDatabaseService db, PropertyContainer propertyContainer, String propertyName){
			this.db = db;
			this.propertyContainer = propertyContainer;
			this.propertyName = propertyName;
		}
	}

	private void retry(ExecutionContext executionContext, Function<ExecutionContext, Object> work, Long times){
		TransactionTemplate template = new TransactionTemplate().retries(times.intValue()).backoff(1, TimeUnit.MILLISECONDS);
		template.with(db).execute(tx -> {
			Lock lock = tx.acquireWriteLock(executionContext.propertyContainer);
			Object result = work.apply(executionContext);
			lock.release();
		});
	}

	private List<Object> insertValueIntoArray(Object oldValue, Long position, Object value) {
		List<Object> values = new ArrayList<>();
		if (oldValue.getClass().isArray())
			values.addAll(new ArrayBackedList(oldValue));
		else
			values.add(oldValue);
		if (position > values.size())
			values.add(value);
		else
			values.add(position.intValue(), value);
		return values;
	}

	private void checkIsPropertyContainer(Object container) {
		if (!(container instanceof PropertyContainer)) throw new RuntimeException("You Must pass Node or Relationship");
	}

	public class AtomicResults {
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