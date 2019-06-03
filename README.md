# RapidMiner Belt

The _RapidMiner Belt_ library implements a labeled two-dimensional data table with support for columns of different types.
It provides utility methods for creating, reading, and modifying such tables.

This copy of RapidMiner Belt is licensed under the Affero General Public License version 3.
See the attached [LICENSE file](LICENSE) for details.

**This is a beta version.**
Be cautious when using the Belt project in production code:
the API is not finalized yet and might undergo smaller changes until the release of version 1.0.

## Table of Contents

- [Concepts and example](#belt-introduction)
- [Creating tables](#belt-creating-tables)
- [Reading tables](#belt-reading-tables)
- [Concurrency](#belt-concurrency)
- [IO](#belt-i-o)
- Appendix
    * [Table properties](#belt-table-properties)
    * [Column types](#belt-column-types)
    * [Dictionaries](#belt-dictionaries)
    * [Column buffers](#belt-column-buffers)
- [Javadoc](https://rapidminer.github.io/belt/apidocs/)

<a name="belt-introduction" />

# Concepts and example

The Belt library aims at being efficient both in terms of runtime and memory usage while ensuring data consistency on the API level.
This is achieved by building the library around the following concepts:

* **Column-oriented design:** a column-oriented data layout allows for using compact representations for the different [column types](#belt-column-types).
* **Immutability:** all columns and tables are immutable. This not only guarantees data integrity but also allows for safely reusing components, e.g., multiple tables can safely reference the same column.
* **Thread-safety:** all public data structures are thread-safe and designed to perform well when used concurrently.
* **Implicit parallelism:** Many of Belt's built-in functionality, such as the transformations shown in the example below, automatically scale out to multiple cores.

<a name="belt-introduction-example" />

Belt provides multiple mechanisms to construct new tables.
For example, one can specify cell values via lambda functions:

```Java
Table table = Builders.newTableBuilder(10)
		.addInt("id", i -> i)
		.addNominal("sensor", i -> String.format("Sensor #%03d", rng.nextInt(3)))
		.addReal("value_a", i -> rng.nextDouble())
		.addReal("value_b", i -> rng.nextDouble())
		.build(context);
```
```text
Table (4x10)
id      | sensor      | value_a | value_b
Integer | Nominal     | Real    | Real   
      0 | Sensor #002 |   0.957 |   0.262
      1 | Sensor #002 |   0.938 |   0.475
      2 | Sensor #001 |   0.515 |   0.585
    ... |         ... |     ... |     ...
      9 | Sensor #001 |   0.197 |   0.753
```

Different transformations are supported.
The code below creates a new column containing the maximum of the two value columns using one of the provided ```apply(...)``` methods.
The result, a buffer of numeric values, can easily added to the table as additional column:


```Java
NumericBuffer max = table.transform("value_a", "value_b")
		.applyNumericToReal(Double::max, context);

table = Builders.newTableBuilder(table)
		.add("max_value", max.toColumn())
		.build(context);
```
```
Table (5x10)
id      | sensor      | value_a | value_b | max_value
Integer | Nominal     | Real    | Real    | Real     
      0 | Sensor #002 |   0.957 |   0.262 |     0.957
      1 | Sensor #002 |   0.938 |   0.475 |     0.938
      2 | Sensor #001 |   0.515 |   0.585 |     0.585
    ... |         ... |     ... |     ... |       ...
      9 | Sensor #001 |   0.197 |   0.753 |     0.753
```

Another supported transformation is the reduction of one or multiple columns to a single value.
For example, the following code computes the column sum for the column created above: 

```Java
double sumOfMax = table.transform("max_value")
		.reduceNumeric(0, Double::sum, context);
```
```
7.562
```

The following sections will discuss the different means to create and work with Belt tables in more detail.
Alternatively, you can jump right into the [Javadoc](https://rapidminer.github.io/belt/apidocs/).
To know more about the implicit parallelism and the `context` used in the examples above you can visit the [concurrency section](#belt-concurrency).

<a name="belt-creating-tables" />

# Creating tables

The Belt library supports both column-wise and row-wise construction of tables.
Furthermore, tables can be derived from existing table, e.g., by selecting row or column subsets.

When creating a new table, **the column-wise construction is preferred:**
it is usually more efficient due to the column-oriented design of the library.
The purpose of the row-wise construction is mostly to support use cases such as reading a row-oriented file format.

## Column-wise construction

To create a new table from scratch, the ```Builders``` class provides the method ```newTableBuilder(int)```.
The returned builder supports multiple methods to add columns to the new table.

The first method used below ```add(String, Column)``` expects an existing column of matching height.
In this example, this column is created from a temporary numeric buffer in which most values are undefined.
The second method ```addReal(String, IntToDoubleFunction)``` constructs a new column of type real by invoking the given operator for values ```i = 0, 1, ..., numberOfRows - 1```.
Similar methods exist for the other column types supported by Belt.

```Java
int height = 10;

NumericBuffer buffer = Buffers.realBuffer(height);
buffer.set(1, Math.PI);

Table table = Builders.newTableBuilder(height)
		.add("From buffer", buffer.toColumn())
		.addReal("From operator", Math::sqrt)
		.build(context);
```
```
Table (2x10)
From buffer | From operator
Real        | Real         
          ? |         0.000
      3.142 |         1.000
          ? |         1.414
        ... |           ...
          ? |         3.000
```

The ```NumericBuffer``` used above is one of many buffer implementations provided by the ```Buffers``` class.
Buffers differ from columns in that they are mutable and can basically be used like an array.
However, they become immutable once ```toColumn()``` is invoked:
**subsequent calls to ```set(...)``` will fail**.

This allows for buffer implementations that do not need to duplicate data in order to guarantee the immutability of the resulting column.
Thus, it is safe to assume that using buffers is as memory-efficient as constructing columns via lambda functions.

For more information about different types of buffers visit the [buffer appendix](#belt-column-buffers).

## Row-wise construction

The ```Writers``` class provides multiple writer implementations.
Specialized ones that support only a certain column type and generic ones that allow to combine different types.

To recreate the example above, we can use the specialized real row writer.
Unlike with the column-wise construction, you need to specify all columns upfront.
On the flip side, specifying the number of rows is optional:

```Java
NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("first", "second"), true);

for (int i = 0; i < 10; i++) {
	writer.move();
	if (i == 1) writer.set(0, Math.PI);
	writer.set(1, Math.sqrt(i));
}

Table table = writer.create();
```
```
Table (2x10)
first | second
Real  | Real  
    ? |  0.000
3.142 |  1.000
    ? |  1.414
  ... |    ...
    ? |  3.000
```

All writers provide two methods:
The ```move()``` method which creates a new row,
and the ```set(int, ...)``` method which allows to set the values of individual cells of the current row.
Cells are referenced by their index which is the same as the corresponding column index.

To recreate the example from the [introductory example](#belt-introduction-example), we will need to use a ```MixedRowWriter``` which supports combining different types.
All writers need to know the table schema in advance.
When working with mixed writers, this is accomplished by providing a separate list of column types:

```Java
MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("id", "sensor", "value_a", "value_b"),
		Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.NOMINAL, ColumnTypes.REAL, ColumnTypes.REAL),
		false);

for (int i = 0; i < 10; i++) {
	writer.move();
	writer.set(0, i);
	writer.set(1, String.format("Sensor #%03d", rng.nextInt(3)));
	writer.set(2, rng.nextDouble());
	writer.set(3, rng.nextDouble());
}

Table table = writer.create();
```
```
Table (4x10)
id      | sensor      | value_a | value_b
Integer | Nominal     | Real    | Real   
      0 | Sensor #002 |   0.805 |   0.124
      1 | Sensor #001 |   0.744 |   0.896
      2 | Sensor #001 |   0.657 |   0.610
    ... |         ... |     ... |     ...
      9 | Sensor #000 |   0.378 |   0.282
```

You might have noticed that the factory methods for writers require an additional Boolean parameter which is set to ```true``` and ```false``` in the first and second example above respectively.
This parameter indicates whether writers should initialize rows as _undefined_ or _missing_ (shown as ```?``` above).
It is set to ```true``` in the first example, since we do not always set all cell values in the loop.
In the second example however, we do set all values in the loop and no initialization is necessary.

Finally, you might have noticed that unlike with the column-wise construction, the row-wise construction does not require the use of an execution context.
The reason for this is that all work is done inside the loop.
In contrast, when constructing columns using lambda function, the work is done by the builder which will use the context for that purpose.

## Deriving tables

The Belt library provides three main mechanisms to derive a table from an existing one.

1. **Column set selection:** creates a new table consisting only of the specified columns.
2. **Row set selection:** creates a new table consisting only of the specified rows.
3. **The table builder:** starts the table building process outlined above from an existing table.

To specify a column set, we can use the ```Table#columns(List<String>)``` method which takes a list of column labels as input.
The selected columns will be reordered according to the order in the list.
The following example takes the table generated at the end of the last section, drops the ```value_b``` column, and moves the ```sensor``` column to the end of the table:

```Java
Table reorderedColumns = table.columns(Arrays.asList("id", "value_a", "sensor"));
```
```
Table (3x10)
id      | value_a | sensor     
Integer | Real    | Nominal    
      0 |   0.805 | Sensor #002
      1 |   0.744 | Sensor #001
      2 |   0.657 | Sensor #001
    ... |     ... |         ...
      9 |   0.378 | Sensor #000
```

Similarly, we can select a set of rows using the ```Table#rows(int[], Context)``` method. 
The following example selects three rows and changes their order (see the ```id``` column in the output): 

```Java
Table threeRows = table.rows(new int[]{2, 0, 7}, context);
```
```
Table (4x3)
id      | sensor      | value_a | value_b
Integer | Nominal     | Real    | Real   
      2 | Sensor #001 |   0.657 |   0.610
      0 | Sensor #002 |   0.805 |   0.124
      7 | Sensor #001 |   0.787 |   0.200
```

This method also supports duplicate row ids.
For example, passing in the index array ```{0, 0, 0}``` instead, would create a 4×3 table containing the first row three times.
As a special case of row selection there are filter methods that only select the rows satisfying a certain condition, and sorting methods to reorder the rows. 

The third mechanism restarts the table building process (without modifying the input table).
It provides all of the methods discussed in the section on column-wise table creation and additional methods that allow to replace or remove columns.
For example, the following code replaces the column ```value_a``` with one scaled by factor ```500```, renames the ```sensor``` column, and again drops the ```value_b``` column:

```Java
NumericBuffer valueAScaled = table.transform("value_a")
		.applyNumericToReal(v -> v * 500, context);

Table complexChanges = Builders.newTableBuilder(table)
		.rename("sensor", "sensor_id")
		.replace("value_a", valueAScaled.toColumn())
		.remove("value_b")
		.build(context);
```
```
Table (3x10)
id      | sensor_id   | value_a
Integer | Nominal     | Real   
      0 | Sensor #002 | 402.686
      1 | Sensor #001 | 371.767
      2 | Sensor #001 | 328.636
    ... |         ... |     ...
      9 | Sensor #000 | 189.064
```


For more details about what information a table holds, visit the [Table properties appendix](#belt-table-properties).

<a name="belt-reading-tables" />

# Reading tables

Tables can be read either column-wise or row-wise.
If possible, column-wise reading is preferred since it allows to better choose the appropriate reader for a certain column.
Due to the column-oriented design of the library, column-wise reading usually also performs better than row-wise reading.

The entry point for both column-wise and row-wise reading is the ```Readers``` class.

## Column-wise reading

There are three possible ways to read columns:

A column can be read as sequence of numbers (i.e. double values) with

```java
NumericReader reader = Readers.numericReader(column);
while(reader.hasRemaining()){
    double value = reader.read();
}
```

Alternatively, a column can be read as sequence of category indices with
 
```java
CategoricalReader reader = Readers.categoricalReader(column);
while(reader.hasRemaining()){
    int index = reader.read();
}
```

As third option, a column can be read as sequence of objects of a specified class with

```java
ObjectReader<String> reader = Readers.objectReader(column, String.class);
while(reader.hasRemaining()){  
	    String value = reader.read();
}
```
 
In every case a reader is constructed and the next value is read via `reader.read()`.
If reading should start from a certain position, 
`reader.setPositon(desiredNextRead - 1)` can be used to move the reader in front of the desired position so that the next value is read at the position.

`NumericReader` and `ObjectReader` instances can be used for any column with **capability** `Capability.NUMERIC_READABLE` and `Capability.OBJECT_READABLE`.
For instance, to check whether a column is numeric-readable, we can call ``column.type().hasCapability(Capability.NUMERIC_READABLE)``.
Please note that a column must not be of **category** `Category.NUMERIC` to be numeric-readable.

In contrast, a `CategoricalReader` instance requires the columns to be of **category** `Category.CATEGORICAL`, which can be checked with ``column.type().category() == Category.CATEGORICAL``.
For further information on different types of columns visit the section on [Column types](#belt-column-types).

When reading categorical columns with a `CategoricalReader` as in the following example, missing values are represented by `CategoricalReader.MISSING_CATEGORY`.

```java
Column column = getColumn();
if (column.type().category() == Column.Category.CATEGORICAL) {
	int firstMissing = -1;
	CategoricalReader reader = Readers.categoricalReader(column);
	for (int i = 0; i < column.size(); i++) {
		if (reader.read() == CategoricalReader.MISSING_CATEGORY) {
			firstMissing = i;
			break;
		}
	}
}

```

When reading with a `NumericReader` missing values are `Double.NaN` and with an `ObjectReader` they are `null`. 

In the following code example, we read columns either as number or as objects.

```java
// All columns are numeric-readable, object-readable, or both
if (column.type().hasCapability(Capability.NUMERIC_READABLE)) {
    NumericReader reader = Readers.numericReader(column);
    while (reader.hasNext()) {
        double value = reader.read();
        handle(value);
    }
} else {
    ObjectReader<Object> reader = Readers.objectReader(column, Object.class);
    while (reader.hasNext()) {
        Object value = reader.read();
        handle(value);
    }
}
```

It is important to know that every column is numeric-readable, object-readable or both.
All object-readable columns can be read with an `ObjectReader<Object>` but if you know more about the column type you can also use a more specialized class.

Note that when reading a categorical column with a categorical reader, the associated object value (as obtained with an object reader) can be retrieved via `column.getDictionary(type).get(categoryIndex)`. 
For more information about dictionaries visit the [dictionary appendix](#belt-dictionaries).

## Row-wise reading

Similar to column-wise reading, there are three standard ways to read row-wise:

A list column can be read as numbers (i.e. double values) with

```java
NumericRowReader reader = Readers.numericRowReader(columns);
while (reader.hasRemaining()) {
	reader.move();
	for (int i = 0; i < reader.width(); i++) {
		double value = reader.get(i);
	}
}
```
 
Alternatively, categorical columns can be read as category indices with
 
```java
CategoricalRowReader reader = Readers.categoricalRowReader(columns);
while (reader.hasRemaining()) {
	reader.move();
	for (int i = 0; i < reader.width(); i++) {
		int value = reader.get(i);
	}
}
```

As third option, columns can be read as objects of a specified class with

```java
ObjectRowReader<String> reader = Readers.objectRowReader(columns, String.class);
while (reader.hasRemaining()) {
	reader.move();
	for (int i = 0; i < reader.width(); i++) {
		String value = reader.get(i);
	}
}
```
   
where now the class must be compatible with all the columns.

For all readers, the `reader.move()` method advances the reader by one row.
After that, row values can be accessed via `reader.get(index)`.
As for column-wise reading, if reading starts from a certain row,
`reader.setPositon(desiredNextRow - 1)` can be used to move the reader in front of the desired row so that the next values are read at the row position.
As before, missing values in `NumericRowReader` are `Double.NaN`,
in `CategoricalRowReader` they are `CategoricalReader.MISSING_CATEGORY`, and in `ObjectRowReader` they are `null`. 
 
Analogous to column-wise reading, all columns to be read by a `NumericRowReader` must be numeric-readable 
and all columns for `ObjectRowReader` must be object-readable. 
In contrast, `CategoricalRowReader` requires the columns to be categorical.
The readability and category can be checked as described for column-wise reading.
For further information on different types of columns visit the section on [Column types](#belt-column-types).  
   
To find all columns in a table with a certain readability property one can use the `table.select()` method as in the following example:

```java
List<Column> numericReadableColumns =
		table.select().withCapability(Column.Capability.NUMERIC_READABLE).columns();
for (NumericRowReader reader = Readers.numericRowReader(numericReadableColumns); reader.hasRemaining(); ) {
	reader.move();
	for (int i = 0; i < reader.width(); i++) {
		double value = reader.get(i);
		handle(value);
	}
}
```

Note that when reading categorical columns with a numeric reader, the associated object values (as obtained with an object reader) can be retrieved via `column.getDictionary(type).get((int) value)`.
For more information about dictionaries visit the [dictionary appendix](#belt-dictionaries).

The next example uses the column selector to find all nominal columns which are readable as String objects.

```java
ColumnSelector columnSelector = table.select().ofTypeId(Column.TypeId.NOMINAL);
List<Column> nominalColumns = columnSelector.columns();
List<String> nominalLabels = columnSelector.labels();
List<String> found = new ArrayList<>();
for (int i = 0; i < nominalColumns.size(); i++) {
	ObjectReader<String> reader = Readers.objectReader(nominalColumns.get(i), String.class);
	while (reader.hasRemaining()) {
		String read = reader.read();
		if (read != null && read.contains("toFind")) {
			found.add(nominalLabels.get(i));
			break;
		}
	}
}
```

Besides the specialized readers there is one other reader that can read all types of columns.

```java
MixedRowReader reader = Readers.mixedRowReader(table);
while (reader.hasRemaining()) {
	reader.move();
	for(int i = 0; i< table.width(); i++){
		ColumnType<?> type = table.column(i).type();
		if(type.category()== Column.Category.CATEGORICAL){
			int index = reader.getIndex(i);
		}else if(type.hasCapability(Column.Capability.OBJECT_READABLE)){
			Object value = reader.getObject(i);
		}else{
			double value = reader.getNumeric(i);
		}
	}
}
```

The method `reader.getNumeric(index)` returns as `double` the numeric value of the index-th column or `Double.NaN` if the column is not numeric-readable.
The method `reader.getIndex(index)` returns the category index of a categorical column or some undefined value if the index-th column is not categorical.
The method `reader.getObject(index)` returns the object value of an object-readable column or `null` if the column is not object-readable.
If a more specialized class of an object-readable column is known, the method `reader.getObject(index, type)` can be used,
e.g. `reader.getObject(5, String.class)` returns the String value.
 
Note that having different kinds of columns in a table does not lead to requiring a mixed row reader for row-wise reading.
Keep in mind that every column is either numeric or object readable.
Thus, you always cover all columns when reading the columns with and without capability `Capability.NUMERIC_READABLE` using one numeric and one object row reader respectively.
  
Even though the row readers implement a row interface, these rows are only views and should never be stored.
The following example will not allow future access to all rows but only the last:

```java
// Don't do this!
List<NumericRow> rows = new ArrayList<>();
NumericRowReader reader = Readers.numericRowReader(column1, column2);
while (reader.hasRemaining()) {
	reader.move();
	rows.add(reader);
}
```

<a name="belt-concurrency" />

# Concurrency

Much of the built-in functionality of the Belt library can scale out to multiple CPU cores.
In particular, most data transformations work concurrently.
It is easy to identify whether a particular API supports parallel processing or not.
If the API requires an execution ```Context```, it can scale the operation out to multiple cores.
If it does not, it will run the code single-threaded unless you manually parallelize it, e.g.,
by directly using the functionality provided by the context.

## The execution context

The ```Context``` class provides methods similar to the ones provided by Java's thread pools.
For instance, the method ```Context#getParallelism()``` specifies the desired parallelism level (number of worker threads used)
and the method ```Context#call(List<Callable>)``` executes a list of functions concurrently.
For a complete list of context methods, please refer to the [Javadoc](https://rapidminer.github.io/belt/apidocs/). 

The ```Context```class differs from thread pool implementations in that it does not necessarily maintain its own pool of worker threads.
The idea is that multiple short-lived ```Context``` instances can share the same long-lived worker pool.
A single ```Context```instance is intended to be used for a single complex computation such as an analytical workflow:
the context manages the resources for that workflow and is typically marked as inactive as soon as the workflow is completed (or aborted).
This does not mean that a new context should be used for each interaction with the Belt library.
A workflow might consists of many such interactions.

Most of the time all you need to do is to pass a context to the library functions.
The library will then automatically decide on a suitable scheduling strategy.
How to use the ```Context```class directly for use cases not covered by these functions will be described later on this page.

## Auto-scaling functions

An example for functions that automatically scale out to multiple cores are the table transformations.
For instance, the following example might be executed concurrently:

```Java
NumericBuffer addOne = table.transform("data")
		.applyNumericToInteger(v -> v + 1, context);
```
Whether the code runs multi-threaded and with how many worker threads depends on:

* **The parallelism level of the context.** At most ```Context#getParalellism()``` worker threads will be used. If the parallelism level is one, the code will run single-threaded.
* **The size of the data table.** Very small data sets might be processed using a single worker thread, since the scheduling overhead for the multi-threaded code might outweigh the performance increase due to the parallelization.
* **The expected workload per data point.** What is considered a _very small data set_ also depends on the expected complexity of the given operator. By default, the Belt library assumes a medium workload such as a string manipulation.

The expected workload per data point can also be specified explicitly using the ```workload(Workload)```method.
For example, there are only a few faster than the simple addition in the example above.
Thus, we can explicitly set the workload ```Workload.SMALL```:

```Java
NumericBuffer addOne = table.transform("data")
		.workload(Workload.SMALL)
		.applyNumericToInteger(v -> v + 1, context);
```

Whereas for expensive arithmetic operations such as ```Math#sin(double)```, a larger workload might be more appropriate:

```Java
NumericBuffer sin = table.transform("data")
		.workload(Workload.MEDIUM)
		.applyNumericToInteger(Math::sin, context); 
```

The workload not only plays a factor when deciding whether to run the core in parallel,
but also when deciding on the actual scheduling strategy.
Thus, it is recommended to specify the workload whenever it is known in advance.
More information on the different workload settings can be found in the Javadoc.

Finally, if you want to keep track of the progress of a computation handed off to the execution context,
you can use the ```callback(DoubleConsumer)```method.
For example, the following code would write the progress to the standard output:

```Java
NumericBuffer sin = table.transform("data")
		.workload(Workload.MEDIUM)
		.callback(p -> System.out.println(String.format("Progress: %d%", p * 100)))
		.applyNumericToInteger(Math::sin, context); 
```

The progress is either in range ```[0, 1]``` or of value ```Double.NAN``` for indeterminate states.
Please note that invocations of the callback function are not synchronized.
For instance, if two threads complete the 4th and 5th batch of a workload at the same time, the callbacks with values ```0.4``` and ```0.5``` will be triggered simultaneously as well.
Thus, in the example above, the progress might be printed out of order.

While it would be possible to extract column statistics, e.g. the number of missing values, from a column using the `Transformer#reduce*` methods, 
there is built-in functionality for that and more sophisticated statistics accessible via the `Statistics` class.

## Custom concurrent code

When you encounter a use case that is not covered by the auto-scaling functions described above,
you can still use the Belt library in a concurrent way.
However, you will have to implement your own scheduling logic.

The library's core data structures, such as buffers, columns, and tables are all thread safe and designed to allow efficient parallel use.
However, temporary constructs such as readers or writers are usually not. 
For example, it is safe to read from one table with two threads, but each thread will require its own reader. 

While you can use the core data structures of the Belt library with any threading framework,
it is recommended to use its ```Context``` implementation.
Using the same context and thus worker pool for all concurrent code will keep all resource management at a central place.
Furthermore, the ```Context``` implementation allows for advanced use cases such as nested submissions.

Let us, as simple example, recreate the computations above as simple single threaded loops.
In order to run them using a context, we will have to wrap these loops in ```Callables```.
The ```addOne```implementation could look like this: 

```Java
Callable<NumericBuffer> addOne = () -> {
	NumericBuffer buffer = Buffers.realBuffer(table.column("data"));
	for (int i = 0; i < buffer.size(); i++) {
		buffer.set(i, buffer.get(i) + 1);
	}
	return buffer;
};
```
The code applying the sinus function can be implemented the same way:

```Java
Callable<NumericBuffer> sin = () -> {
	NumericBuffer buffer = Buffers.realBuffer(table.column("data"));
	for (int i = 0; i < buffer.size(); i++) {
		buffer.set(i, Math.sin(i));
	}
	return buffer;
};
```

To run the two loops concurrently, we can simply use the ```Context#call(List<Callable>)```method:

```Java
List<NumericBuffer> buffers = context.call(Arrays.asList(addOne, sin));
```

You might wonder why this method is blocking, i.e., does not return until all computations have completed.
Usually, such a design can cause issues when invoking ```call(List<Callable>)``` from inside the worker thread:
the worker thread has to wait for other worker threads to complete their computations.
This might prevent the worker pool to be fully utilized or even cause deadlocks.

The implementation of the context prevents this problem by checking whether or not the invoking thread is a worker thread.
If it is, the thread will take part of the computation. 
In particular, no deadlocks can occur due to nested submissions.
To summarize, writing code like this using the Belt library is safe.

```Java
Callable<List<NumericBuffer>> nested = () -> context.call(Arrays.asList(addOne, sin));
List<List<NumericBuffer>> buffers = context.call(Arrays.asList(nested));
```

The example above makes little sense on its own. 
However, the same mechanism allows mixing single-threaded computations in the foreground and auto-scaling methods,
while using only the resources managed by the context.
Consider the following method that does some single-threaded work in the foreground and then calls one of the auto-scaling methods of the Belt library:

```Java
public static Table mixed(Table table, Context context) {
	// Run in foreground
	NumericBuffer buffer = Buffers.realBuffer(table.column("column_a"));
	for (int i = 0; i < buffer.size(); i++) {
		// Single threaded operation
	}

	// Use auto-scaling methods
	buffer = new RowTransformer(Arrays.asList(buffer.toColumn(), table.column("column_b")))
			.applyNumericToReal((a, b) -> { /* Concurrent operation */ }, context);

	// Add result to input table
	return Builders.newTableBuilder(table)
			.add("result", buffer.toColumn())
			.build(context);
}
```

By invoking the code as follows all operations are executed inside the worker pool managed by the context.
Due to Belt's scheduling logic, the nested call to ```RowTransformer#applyNumericToReal(Column, Column)``` in the ```mixed(Table Context)``` method shown above will not block any of the worker threads:

```Java
context.call(Arrays.asList(() -> mixed(table, context)));
```

<a name="belt-i-o" />

# IO

The Belt library will provide a low-level API for efficient IO.
However, this functionality is still a work-in-progress.
This section will be updated once the API has been completed.

# Appendix

<a name="belt-table-properties" />

## Table properties

Tables consist of labelled columns and optional meta data, e.g., for annotations or to assign roles.

### Column labels

The list of column labels or column names can be accessed with `table.labels()` and contains as many elements as 
`table.width()`. To check if a column with a certain label is part of a table, use `table.contains(label)`. If the label 
is part of the table, its position in the table can be ascertained with `table.index(label)`.

Finding all column labels with certain properties can be done with the `table.select()` method, for example

```java
List<String> numericColumnsWithoutRole = table.select()
		.withoutMetaData(ColumnRole.class)
		.ofCategory(Column.Category.NUMERIC)
		.labels();
```

### Column access

For labels contained in the table, the associated column is accessible via `table.column(label)` and has the 
size `table.height()`. Alternatively, columns can be accessed by index via `table.column(index)` where the index is
between 0 (inclusive) and `table.width()` (exclusive).
 
To obtain an (immutable) list of all columns, the method `table.columnList()` can be used. A list of columns with 
certain properties can be obtained with the select method

```java
List<Column> numericColumnsWithoutRole = table.select()
		.withoutMetaData(ColumnRole.class)
		.ofCategory(Column.Category.NUMERIC)
		.columns();
```

### Column meta data

There are three built-in types of `ColumnMetaData`: 

* `ColumnRole` which contains a fixed set of roles for columns
* `ColumnAnnotation` which can be used to associate text, e.g. descriptions, with a column
* `ColumnReference` which can be used to indicate a relationship between columns

Further classes of `ColumnMetaData` can be constructed but they must be immutable.

The list of meta data associated with a column can be obtained via `table.getMetaData(label)`. 
If one is only interested in meta data of a certain type, e.g. column annotations, one can use `table.getMetaData(label, ColumnAnnotation.class)` 
to get the list of annotations associated with a column with a certain label. 
If a meta datum is unique for each column,
one can use `table.getFirstMetaData(label, ColumnRole.class)` to get that unique meta datum or `null`.
Column roles and column references are examples of unique column meta data.

To find columns that have certain meta data, the select method can be used

```java
List<String> columnsWithRoleButNotCluster = table.select()
        .withMetaData(ColumnRole.class)
        .withoutMetaData(ColumnRole.OUTLIER)
        .labels();
```

To change the meta data associated with a column in a certain table, a new table must be constructed via a table builder.
Inside the builder, the methods `TableBuilder#addMetaData` and `TableBuilder#clearMetaData` can be used. 
For more details about creating new tables from old ones using a table builder visit the [table creation section](#belt-creating-tables).


<a name="belt-column-types" />

## Column types

Each column has a type, accessible by `column.type()`.
Types are grouped into **categories** and have different **capabilities** such as being *numeric-readable* or *sortable*.
 
#### Category

Columns are divided in three categories: numeric, categorical and object columns. 
To determine to which category a column belongs, use `column.type().category()`.
 * `NUMERIC` columns contain numeric data, such as real or integer numbers.
 * `CATEGORICAL` columns contain non-unique integer indices paired with an index mapping to a complex type. 
 The index mapping is called a dictionary. 
 A dictionary of two or fewer values can additionally know if a value is positive or negative. 
 It is important to note that not all values in the dictionary are required to appear in the data.
 For more information about dictionaries refer to [Dictionaries](#belt-dictionaries).
 * `OBJECT` columns contain complex types such as instants of time.
 In contrast to categorical columns, object columns contain data that usually does not define a set of categories.
 For example date-time data usually contains so many different values that building a dictionary is not feasible in general.

#### Capabilities

Columns can have the capabilities `NUMERIC_READABLE`, `OBJECT_READABLE` and `SORTABLE`. 
To check if a column has a certain capability use `column.type().hasCapability(capability)`.

Every column is either numeric-readable or object-readable or both. 
Numeric columns are numeric-readable, object columns are object-readable, and categorical columns are both. 
Columns can be readable in different ways, as shown by the time columns in the following section.

Checking the capabilities of one or multiple columns is important for picking the correct reader.
See [Reading tables](#belt-reading-tables) for details.

While all built-in column types are sortable, for some cell types sorting might not make sense. 
A column must be sortable when it is used to sort by in the `table.sort` methods. 
All columns that have a non-zero `Comparator` accessible via `column.type().comparator()` are sortable but there are columns that are sortable without supplying a comparator, for example real columns.

#### Type id

All the built-in types described in detail in the next section are associated with the type ids `REAL`, `INTEGER`, `NOMINAL`, `DATE_TIME` or `TIME`. 
Other types have `CUSTOM` as the `TypeId` which is accessible via `column.type().id()`.
Custom types are described via an additional String with `column.type().customTypeID()` which is `null` for all built-in types.
 
#### Element type

For object and categorical columns, the type of their elements can be accessed via `column.type().elementType()`. 
For instance, nominal columns have the element type `String.class`.
 
### Built-in types

The following column types are built-in.

Type Id | Description | Category | Capabilities | Element Type
--- | --- | --- | --- | ---
REAL | `double` values | NUMERIC | NUMERIC_READABLE, SORTABLE | `Void.class`
INTEGER | `double` values without fractional digits | NUMERIC | NUMERIC_READABLE, SORTABLE | `Void.class`
NOMINAL | `int` category indices together with a dictionary of `String` values | CATEGORICAL | NUMERIC_READABLE, OBJECT_READABLE, SORTABLE | `String.class`
DATE_TIME | Java `Instant` objects | OBJECT | OBJECT_READABLE, SORTABLE | `Instant.class`
TIME | Java `LocalTime` objects | OBJECT | OBJECT_READABLE, NUMERIC_READABLE, SORTABLE | `LocalTime.class`

Date-time columns are the only built-in columns that are not numeric-readable.
Time columns are numeric-readable as nano-seconds since 00:00. 


### Custom types

Custom columns can either be object columns or categorical columns. 
Custom column types can be created via the `ColumnTypes` class, for example 

```java
ColumnType<Coordinate> coordinateObjectType = ColumnTypes.objectType("com.mypage.type.coordinate",
			Coordinate.class, comparator);
```

or

```java
ColumnType<Emoji> emojiCategoricalType = ColumnTypes.categoricalType("com.mypage.type.emoji",
		    Emoji.class, null);
```

If columns of the custom type should not be sortable, the comparator can be `null`. 
The element types, e.g. `Coordinate.class` and `Emoji.class` in our examples, must be immutable, otherwise the safe sharing between tables might break.

To create a custom column, use an object or categorical buffer of the appropriate type as follows

```java
ObjectBuffer<Coordinate> buffer = Buffers.objectBuffer(4);
buffer.set(0, new Coordinate(400.3, 11.2));
buffer.set(2, new Coordinate(400.3, 11.2));
Column column = buffer.toColumn(coordinateObjectType);
```

or


```java
CategoricalBuffer<Emoji> buffer = Buffers.categoricalBuffer(3);
buffer.set(0, emoji1);
buffer.set(2, emoji2);
Column column = buffer.toColumn(emojiCategoricalType);
```

Alternatively, the custom types can be used inside a table builder

```java
Table table = Builders.newTableBuilder(5)
		.addObject("coordinates", i -> new Coordinate(i * 33, i * 55), coordinateObjectType)
		.addCategorical("emojis", i -> emojis[i], emojiCategoricalType)
		.build(context);
```

<a name="belt-dictionaries" />

## Dictionaries

All categorical columns (see [Column types](#belt-column-types)) have dictionaries. 
Dictionaries are a mapping from category indices to object values. 
Every assigned category index is associated to a different object value.
If a category index is not assigned to an object value, it is assumed to be assigned to `null`.
The category index `CategoricalReader.MISSING_CATEGORY` is always unassigned and is the category index that stands for a missing value.

For example, the following nominal column

```
Nominal Column (5)
(green, red, ?, red, ?) 
```

could have the underlying category indices

```
[1, 2, 0, 2, 0] 
```

and the dictionary

```
{1 -> green, 2 -> red} 
```

### Accessing dictionaries

To access a dictionary of a categorical column, use the method `column.getDictionary(type)` where the type is the element type of the column or a super type. 
For example, for the nominal column above, type could be `String.class` or `Object.class`.

If you have a category index and want to find out the associated object value, use `dictionary.get(index)`. 
This method returns `null` for unassigned indices such as `CategoricalReader.MISSING_CATEGORY`.

If you require the reverse mapping from object values to category indices, you can create one by using `dictionary.createInverse()`.

To iterate through all assigned object values together with their category indices, the dictionary iterator can be used.
Continuing with the example above we get:

```java
Dictionary<String> dictionary = column.getDictionary(String.class);
for (Dictionary.Entry<String> entry : dictionary) {
	System.out.println(entry.getIndex() + " -> " + entry.getValue());
}
```

```
1 -> green
2 -> red
```

### Unused values

Not every object value in a dictionary must appear in the data. 
But the objects in the dictionary give an upper bound for the different object values in the column.
For example, the nominal column above could have the following underlying category indices and dictionary:

```
[1, 3, 0, 3, 0] 
```

```
{1 -> green, 2 -> blue, 3 -> red} 
```

To get rid of unused dictionary entries, one can use the method `Columns#removeUnusedDictionaryValues`.
There are two options: first, just remove the unused values or compact the dictionary so that the remaining category indices are sequential.

```java
Column newColumn = Columns.removeUnusedDictionaryValues(column, Columns.CleanupOption.REMOVE, context);
```

creates a new column with the same underlying category indices and the dictionary

```
{1 -> green, 3 -> red} 
```

Second, 

```java
Column newCompactColumn = Columns.removeUnusedDictionaryValues(column, Columns.CleanupOption.COMPACT, context);
```

results in a new column with the dictionary

```
{1 -> green, 2 -> red} 
```

and adjusted category indices. 

If gaps between used category indices cause no problems, the first option should be preferred.
In case the gaps need to be removed later on, `Columns.compactDictionary(newColumn)` can be used to obtain the `newColumn2` from above.

### Aligning dictionaries
If the category indices of two similar categorical columns with different dictionaries should be compared, these dictionaries need to be aligned.
The methods `Columns#changeDictionary` and `Columns#mergeDictionary` help with that.

Assume we have `columnA` from the beginning of the section with the following dictionary:

```
Nominal Column (5)
(green, red, ?, red, ?) 
```

```
{1 -> green, 2 -> red} 
```

Furthermore, assume there is a `columnB` that has similar entries and the dictionary below:

``` 
Nominal Column (6)
(?, red, yellow, green, ?, green)
```

``` 
{1 -> red, 2 -> yellow, 3 -> green}
```

Then the following code results in a new columnB with the dictionary shown below:

``` 
Column newColumnB = Columns.mergeDictionary(columnB, columnA);
```

```
Nominal Column (6)
(?, red, yellow, green, ?, green)
```

```
{1 -> green, 2 -> red, 3 -> yellow} 
```

The dictionary starts with the same values as the dictionary of `columnA`.

In contrast, the following code results in another new columnB with the dictionary shown below:

```java
Column newColumnB = Columns.changeDictionary(columnB, columnA);
```

``` 
Nominal Column (6)
(?, red, ?, green, ?, green)
```

``` 
{1 -> green, 2 -> red} 
```

The dictionary is exactly the same as the dictionary for `columnA`.
Since the dictionary from `columnA` does not contain `yellow`, the new columnB has a missing value instead of `yellow`.

Note that the columns do not have to be the same size.

### Boolean dictionaries

When dictionaries are boolean, they know which values are positive and which are negative. 
Whether or not a dictionary is boolean can be checked via `Dictionary#isBoolean`.
Only dictionaries with at most two values can be boolean.

Using `Columns#toBooleanColumn`, a column can be made into a boolean categorical column if it satisfies the requirements which can be checked via `Columns#isAtMostBicategoricalColumn`.

For our running example

```
Nominal Column (5)
(green, red, ?, red, ?) 
```

the following code leads to a boolean nominal column with the additional information that `green` is positive:

```java
Column booleanColumn = Columns.toBoolean(column, "green");
```

If there is only one value in the dictionary and it is supposed to be negative, `Columns.toBoolean(column, null)` can be called.

Boolean information can be accessed as in the following example

```java
Dictionary<String> dictionary = booleanColumn.getDictionary(String.class);
if(dictionary.isBoolean()){
	if(dictionary.hasPositive()){
		String positiveValue = dictionary.get(dictionary.getPositiveIndex());
		System.out.println("positive: " + positiveValue);
	}
	if(dictionary.hasNegative()){
		String negativeValue = dictionary.get(dictionary.getNegativeIndex());
		System.out.println("negative: " + negativeValue);
	}
}
```

Note that even if a value is positive or negative in the dictionary, it could still be absent in the data.
For example, if the dictionary in the example above with a ```green``` positive and a ```red``` negative, it is possible that the data only contains red and missing values.

<a name="belt-column-buffers" />

## Column buffers

Column buffers are temporary data structures that help with the construction of new columns.
One can think of them as mutable arrays that can be frozen and converted into immutable columns.

Buffers are created with a fixed size, and afterwards one can set or access values at certain indices.

```java
NumericBuffer buffer = Buffers.realBuffer(10);
for (int i = 0; i < 10; i++) {
	buffer.set(i, i + 0.123);
}
Column column = buffer.toColumn();
```

```text
Real Column (10)
(0.123, 1.123, 2.123, 3.123, 4.123, 5.123, 6.123, 7.123, 8.123, 9.123)
```

To create a column from a buffer, `buffer.toColumn` is called. 
After that, subsequent calls to the `set`-method will fail. 
 
```java
buffer.set(4, 2.71);
```

```text
java.lang.IllegalStateException: Buffer cannot be changed after used to create column
	at com.rapidminer.belt.buffer.RealBuffer.set(RealBuffer.java:72)
```
 
Instead of creating an empty buffer, a buffer can be created from a column. 
This is helpful in cases where only a few values of a column need to be changed. 
Of course, the column type needs to be compatible with the buffer type.
 
The different types of buffers have various special properties described below.
 
### Real buffers
 
A real buffer can be thought of as a `double` array. 
 
```java
NumericBuffer buffer = Buffers.realBuffer(10, false);
Random random = new Random(123);
for (int i = 0; i < buffer.size(); i++) {
	buffer.set(i, random.nextInt(100) + Math.PI);
}
buffer.set(2, Double.NaN);
buffer.set(7, Double.POSITIVE_INFINITY);
buffer.set(1, Double.NEGATIVE_INFINITY);
Column column = buffer.toColumn();
```

```
Real Column (10)
(85.142, -Infinity, ?, 92.142, 98.142, 60.142, 37.142, Infinity, 88.142, 56.142)
```

In the example above, the method `Buffers.realBuffer(size, false)` is used. 
This turns off the initialization of the buffer and should be done in every case where the `set(index, value)` method will be called for every index. 
`Buffers.realBuffer(size, true)` or just `Buffers.realBuffer(size)` initializes the buffer to missing values so that the value at every unset index is a missing value.  
Missing values can be set via `buffer.set(index, Double.NaN)` and it is also possible to set infinite values.

When creating a real buffer from a column via `Buffers.realBuffer(column)`, the column must have the capability `NUMERIC_READABLE` (see [Column types](#belt-column-types)). 
If a value should be changed depending on the current value, the method `buffer.get(index)` which returns a double value can be used.
 
### Integer buffers
 
An integer buffer is similar to a real buffer but in order to ensure `double` values without fractional digits required for `INTEGER` columns (see [Column types](#belt-column-types)), the input is rounded.
  
```java
NumericBuffer buffer = Buffers.integerBuffer(10, true);
buffer.set(2, 3.0);
buffer.set(1, 4.0);
buffer.set(9, 3.14);
buffer.set(5, 2.718);
buffer.set(2, Double.NaN);
buffer.set(7, Double.POSITIVE_INFINITY);
buffer.set(6, Double.NEGATIVE_INFINITY);
Column column = buffer.toColumn();
```  

```
Integer Column (10)
(?, 4, ?, ?, ?, 3, -Infinity, Infinity, ?, 3)
```

Here, both `3.14` and `2.718` are rounded to `3`. 
As for real buffers, it is possible to set infinite values and missing values to `Double.NEGATIVE_INFINITY`, `Double.POSITIVE_INFINITY`, and `Double.NaN` respectively.
Since `Buffers.integerBuffer(size, true)` is used, the buffer is initialized and all unset values, e.g., at index 0, are missing. 
`Buffer.integerBuffer(size, false)` should be used in case every value will be set.
 
To ascertain if a `NumericBuffer` is a real or integer buffer, the method `buffer.type()` can be used.

When creating a real buffer from a column via `Buffers.realBuffer(column)`, the column must have the capability `NUMERIC_READABLE` (see [Column types](column_types.md)), and all values will be rounded.
If a value should be changed depending on the current value, the method `buffer.get(index)` which returns a double value can be used.
 
<a name="belt-categorical-buffers" />
 
### Categorical buffers
 
Categorical buffers are for creating categorical columns.
See [Column types](#belt-column-types) for more details, e.g., on nominal columns.
  
```java
CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 3);
buffer.set(0, "red");
buffer.set(2, "blue");
buffer.set(4, "green");
buffer.set(5, "blue");
buffer.set(9, "red");
buffer.set(7, "blue");
buffer.set(6, "green");
buffer.set(7, null);
Column column = buffer.toColumn(ColumnTypes.NOMINAL);
```

```
Nominal Column (10)
(red, ?, blue, ?, green, blue, green, ?, ?, red)
```

In this example, categorical buffers are created with `Buffers.categoricalBuffer(size)` or `Buffers.categoricalBuffer(size, categories)`. 
To obtain better compression, the method with the categories parameter should be used assuming an upper bound of the different (non-null) values is known.
When using `Buffers.categoricalBuffer(size)` there is no limit to the different values, but often a bound can be known. 
If more different values are set than the maximum allowed by the compression, the `set` method will throw an exception. 
If a small number of categories is chosen and the goal is to stop setting values if this number is reached, the `setSave(index, value)`-method can be used instead, 
which returns `false` if the limit is reached. Looking up the number of different values already encountered is possible with `buffer.differentValues()`. 
 
As in the example above, all values at indices that are not set are automatically missing. 
To explicitly set a missing value, `buffer.set(index, null)` can be used. 
Creating a column at the end requires a `ColumnType` to specify what kind of categorical column should be created, e.g. a nominal column in our example.
In the case where at most two (non-null) values are set and they have a positive/negative assignment, `buffer.toBooleanColumn(type, positiveValue)` can be called instead. 
For example, if we change our code above to never use `"blue"` then we could use `buffer.toBooleanColumn(ColumnTypes.NOMINAL, "green")` to have `"green"` as positive value and `"red"` as negative one.
If only one negative value is set, `buffer.toBooleanColumn(type, null)` can be used.
  
When creating a categorical buffer from a column via `Buffers.categoricalBuffer(column, type)` the column must be categorical. 
The `type` can be a super-type of the element type of the column. 
For example with the `column` created in our example above, one could do
   
```java
CategoricalBuffer<Object> buffer2 = Buffers.categoricalBuffer(column, Object.class);
```

As for empty buffers, if the maximum number of categories is known, the method `Buffers.categoricalBuffer(column, type, categories)` should be used instead. 
However, the categories of the original column must be considered as starting point for the counting. 
As for the other buffers, checking the current value at an index is possible with `buffer.get(index)`.

   
### Time buffers
 
Time buffers are used to create time columns, see [Column types](#belt-column-types).
 
```java
TimeBuffer buffer = Buffers.timeBuffer(10, true);
buffer.set(0, LocalTime.NOON);
buffer.set(2, LocalTime.MIDNIGHT);
buffer.set(2, null);
buffer.set(7, 45200100003005L);
buffer.set(8, LocalTime.ofNanoOfDay(45200100003005L));
buffer.set(5, LocalTime.MIDNIGHT);
Column column = buffer.toColumn();
```

```
Time Column (10)
(12:00, ?, ?, ?, ?, 00:00, ?, 12:33:20.100003005, 12:33:20.100003005, ?)
```

As for integer buffer, `Buffers.timeBuffer(size, true)` or `Buffers.timeBuffer(size)` creates a buffer with all values initially missing value.
If all index values are set, `Buffers.timeBuffer(size, false)` is preferred.

The example above shows how it is possible to set values in a time buffer either by setting `LocalTime` objects or by setting the value as nanoseconds of the day `long` value directly. 
In case the `long` value is at hand, the last version should be preferred over creating a `LocalTime` object just to set the value.
In order to set missing values, `buffer.set(index, null)` should be used.

To create a column from the buffer, `buffer.toColumn` is called. 
To create a buffer from a column with `Buffers.timeBuffer(column)` the column must be a time column. 
Values can be accessed via `buffer.get(index)` but only as `LocalTime` object and not as raw nanoseconds.
 
### Date-time buffers

Date-time buffers are for creating date-time columns, see [Column types](#belt-column-types). 

```java
DateTimeBuffer buffer = Buffers.dateTimeBuffer(10, false);
buffer.set(1, Instant.ofEpochMilli(1549494662279L));
buffer.set(2, Instant.EPOCH);
buffer.set(2, null);
buffer.set(5, Instant.ofEpochSecond(1549454518));
buffer.set(7, 1549454518L);
buffer.set(8, 1549454518L, 254167070);
Column column = buffer.toColumn();
```

```
Date-Time Column (10)
(?, 2019-02-06T23:11:02Z, ?, ?, ?, 2019-02-06T12:01:58Z, ?, 2019-02-06T12:01:58Z, 2019-02-06T12:01:58Z, ?)
```

Date-time buffers (and columns) can have two different levels of precision, either precision to the nearest second or to the nearest nanosecond.
The precision needs to be specified when the buffer is created.
In the example above, `Buffers.dateTimeBuffer(size, false)` creates a buffer without nanosecond-precision while `Buffers.dateTimeBuffer(size, true)` is precise to the next nanosecond.
 
If no nanosecond-precision is required, the buffer will disregard all nanosecond information. 
Data can be set either by setting `Instant` objects, like in the example at indices 1, 2 and 5,
by setting the epoch seconds directly like at index 7,
or by setting the epoch seconds and additional nanoseconds like at index 8.
In the example above, the values at indices 7 and 8 are the same, even though nanoseconds were specified with `buffer.set(index, epochSeconds, nanoseconds)`,
because only second-precision was requested.
Similarly, only the seconds data of the input at index 1 is stored. 
If the code of the example is changed to `Buffers.dateTimeBuffer(10, true)` the resulting column is
  
``` 
Date-Time Column (10)
(?, 2019-02-06T23:11:02.279Z, ?, ?, ?, 2019-02-06T12:01:58Z, ?, 2019-02-06T12:01:58Z, 2019-02-06T12:01:58.254167070Z, ?)
```

which differs from the previous result at indices 1 and 8.

If the data is not expected to have millisecond or nanosecond precision then the date-time buffer should be created without sub-second-precision to save memory. 
In cases where the data is present as primitive values, `set(index, long)` or `set(index, long, int)` should be preferred over creating an instant first like at index 5 in the example above. 
Missing values are again set by `buffer.set(index, null)`.

As for real buffers, date-time buffers are initially filled with missing values except if `Buffers.dateTimeBuffer(size, precision, false)` is used. 
No initialization should be specified in cases where every value is set anyway.

When creating a buffer from a column with `Buffers.dateTimeBuffer(column)` the column must be a date-time column. 
The precision of the buffer will match the precision of the data in the column. 
Values can be accessed with `buffer.get(index)` but only as `Instant` objects and not as raw seconds or nanoseconds data.

 [//]: # (TODO: Currently the precision cannot be accessed and neither can one change the precision to one different from the column - Missing Feature??)


### Object buffers

Object buffers are used to create object columns. 
Currently, the only built-in object column types are time and date-time which have their own buffer.
So object buffers are only of interest for custom column types.
 
```java
ObjectBuffer<String> buffer = Buffers.objectBuffer(10);
for (int i = 0; i < buffer.size(); i++) {
	buffer.set(i, "value_" + i);
}
buffer.set(3, null);
Column column = buffer.toColumn(customStringType);
```

``` 
Custom Column (10)
(value_0, value_1, value_2, ?, value_4, value_5, value_6, value_7, value_8, value_9)
```

In the example, the `customStringType` must be created beforehand as described in the Custom Types section in [Column types](#belt-column-types).
Missing values are again set via `null`.

When creating an object buffer from a column via `Buffers.objectBuffer(column, type)`, the column must be object-readable and the type a super-type of the column. 
For example, 
 
```java
ObjectBuffer buffer2 = Buffers.objectBuffer(column, Object.class);
``` 
 
would be possible for the `column` created above or the one from the [Categorical buffers section](#belt-categorical-buffers) above.