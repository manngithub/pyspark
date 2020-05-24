# Getting to know PySpark
### Using Spark in Python:
The first step in using Spark is connecting to a cluster. 
Creating the connection is as simple as creating an instance of the SparkContext class. The class constructor takes a few optional arguments that allow you to specify the attributes of the cluster you're connecting to.
An object holding all these attributes can be created with the SparkConf() constructor. 

### Using DataFrames:
To start working with Spark DataFrames, you first have to create a SparkSession object from your SparkContext. You can think of the SparkContext as your connection to the cluster and the SparkSession as your interface with that connection.

### Creating a SparkSession:
This recipe introduces the new SparkSession class from Spark 2.0, which provides a unified entry point for all of the various Context classes previously found in Spark 1.x.

The SparkSession class is a new feature of Spark 2.0 which streamlines the number of configuration and helper classes you need to instantiate before writing Spark applications. SparkSession provides a single entry point to perform many operations that were previously scattered across multiple classes, and also provides accessor methods to these older classes for maximum compatibility.

In interactive environments, such as the Spark Shell or interactive notebooks, a SparkSession will already be created for you in a variable named spark. For consistency, you should use this name when you create one in your own application. You can create a new SparkSession through a Builder pattern which uses a "fluent interface" style of coding to build a new object by chaining methods together.

We've already created a SparkSession for you called spark, but what if you're not sure there already is one? Creating multiple SparkSessions and SparkContexts can cause issues, so it's best practice to use the SparkSession.builder.getOrCreate() method. This returns an existing SparkSession if there's already one in the environment, or creates a new one if necessary!
```sh
First import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

Create my_spark:
my_spark = SparkSession.builder.master("local").appName("MLmodel").config("spark.some.config.option","some-value").getOrCreate()

Print my_spark:
print(my_spark)
```

### pyspark.sql module: 
(think of module as .py file which may contain multiple classes and their methods). There are different classes - 
- pyspark.sql.SparkSession Main entry point for DataFrame and SQL functionality.
- pyspark.sql.DataFrame A distributed collection of data grouped into named columns.
- pyspark.sql.Column A column expression in a DataFrame.
- pyspark.sql.Row A row of data in a DataFrame.
- pyspark.sql.GroupedData Aggregation methods, returned by DataFrame.groupBy().
- pyspark.sql.DataFrameNaFunctions Methods for handling missing data (null values).
- pyspark.sql.DataFrameStatFunctions Methods for statistics functionality.
- pyspark.sql.functions List of built-in functions available for DataFrame.
- pyspark.sql.types List of data types available.
- pyspark.sql.Window For working with window functions.

Note that there are classes insides the class in this module.

```sh
Print the tables in the catalog:
print(my_spark.catalog.listTables())
```

### Are you query-ious?:
Running a query on any table is as easy as using the .sql() method on your SparkSession. This method takes a string containing the query and returns a DataFrame with the results!
```sh
query = "FROM flights SELECT * LIMIT 10"

Get the first 10 rows of flights:
flights10 = spark.sql(query)

Show the results:
flights10.show()
```

### Pandafy a Spark DataFrame:
Sometimes it makes sense to then take that table and work with it locally using a tool like pandas. Spark DataFrames make that easy with the .toPandas() method. 
```sh
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

Run the query:
flight_counts = spark.sql(query)

Convert the results to a pandas DataFrame:
pd_counts = flight_counts.toPandas()

Print the head of pd_counts:
print(pd_counts.head())
```

Put some Spark in your data:
Put a pandas DataFrame into a Spark cluster! The SparkSession class has a method for this as well. The .createDataFrame() method takes a pandas DataFrame and returns a Spark DataFrame. The output of this method is stored locally, not in the SparkSession catalog. 

This means that you can use all the Spark DataFrame methods on it, but you can't access the data in other contexts. For example, a SQL query (using the .sql() method) that references your DataFrame will throw an error. To access the data in this way, you have to save it as a temporary table. You can do this using the .createTempView() Spark DataFrame method, which takes as its only argument the name of the temporary table you'd like to register. This method registers the DataFrame as a table in the catalog, but as this table is temporary, it can only be accessed from the specific SparkSession used to create the Spark DataFrame. There is also the method .createOrReplaceTempView(). This safely creates a new temporary table if nothing was there before, or updates an existing table if one was already defined. You'll use this method to avoid running into problems with duplicate tables.
```sh
Create pd_temp:
pd_temp = pd.DataFrame(np.random.random(10))

Create spark_temp from pd_temp:
spark_temp = spark.createDataFrame(pd_temp)

Examine the tables in the catalog:
print(spark.catalog.listTables())

Add spark_temp to the catalog:
spark_temp.createOrReplaceTempView("temp")

Examine the tables in the catalog again:
print(spark.catalog.listTables())
```

Read csv files: SparkSession method read.csv()
```sh
file_path = "/usr/local/share/datasets/airports.csv"

Read in the airports data:
#airports = spark.read.csv(file_path, header=True)
airports = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(r"filename.csv")

Show the data:
airports.show()
```
### Get a summary of the data
```sh
airports.describe().select("Summary","Pregnancies","Glucose","BloodPressure").show()
```
Note that "Summary" column will provide metrics like (count, mean, stddev, min, max)

# Manipulating Data
Spark Dataframe:
Updating a Spark DataFrame is somewhat different than working in pandas because the Spark DataFrame is immutable. This means that it can't be changed, and so columns can't be updated in place.

### Create a Spark Dataframe from table:
```sh
flights = spark.table("flights")

Shape of the dataframe:
print((flights.count(), len(flights.columns)))
```
Creating column:
Let's look at performing column-wise operations. In Spark you can do this using the .withColumn() method, which takes two arguments. First, a string with the name of your new column, and second the new column itself.
```sh
df = df.withColumn("newCol", df.oldCol + 1)
```
Rename the column:
```sh
df = df.withColumnRenamed("oldname","newname")
```
### Replace 0 with NaN for a particular column 'col'
```sh
import numpy as np
from pyspark.sql.functions import when
df = df.withColumn("col", when(df.col == 0, np.nan).otherwise(df.col))
```

### Impute NaN
Using Imputer:
```sh
from pyspark.ml.feature import Imputer
imputer = Imputer(inputCols = ["col1", "col2", "col3", ...], outputCols = ["col1", "col2", "col3", ...])
model = imputer.fit(df)
df = model.transform(df)
df.show()
```

### Filtering data in Spark Dataframe:
Let's take a look at the .filter() method. As you might suspect, this is the Spark counterpart of SQL's WHERE clause. The .filter() method takes either a Spark Column of boolean (True/False) values or the WHERE clause of a SQL expression as a string. For example, the following two expressions will produce the same output:
```sh
flights.filter(flights.air_time > 120).show()
flights.filter("air_time > 120").show()
```
Note that .filter() will reduce the size of dataframe based on the filter condition. But it will return all the columns in the dataframe. Whereas .select() method will return only those columns which are specified. We can use multiple conditions inside .filter() method, for example, remove missing values from different columns and returning the dataframe:
```sh
flights = flights.filter("year is not NULL and dep_delay is not NULL and tailnum is not NULL")
```

### Data Selection:
The Spark variant of SQL's SELECT is the .select() method. This method takes multiple arguments - one for each column you want to select. These arguments can either be the column name as a string (one for each column) or a column object (using the df.colName syntax). When you pass a column object, you can perform operations like addition or subtraction on the column to change the data contained in it, much like inside .withColumn().

The difference between .select() and .withColumn() methods is that .select() returns only the columns you specify, while .withColumn() returns all the columns of the DataFrame in addition to the one you defined. It's often a good idea to drop columns you don't need at the beginning of an operation so that you're not dragging around extra data as you're wrangling. In this case, you would use .select() and not .withColumn().
```sh
Select the first set of columns:
selected1 = flights.select("tailnum", "origin", "dest")

Select the second set of columns:
temp = flights.select(flights.origin, flights.dest, flights.carrier)

Define first filter:
filterA = flights.origin == "SEA"

Define second filter:
filterB = flights.dest == "PDX"

Filter the data, first by filterA then by filterB:
selected2 = temp.filter(filterA).filter(filterB)
```
Note that if you pass a condition inside .select() method then you will get boolean column of same size as original column. This behavior is different from .filter() method.

Similar to SQL, you can also use the .select() method to perform column-wise operations. When you're selecting a column using the df.colName notation, you can perform any column operation and the .select() method will return the transformed column. For example,
```sh
flights.select(flights.air_time/60)
```
returns a column of flight durations in hours instead of minutes. You can also use the .alias() method to rename a column you're selecting. So if you wanted to .select() the column duration_hrs (which isn't in your DataFrame) you could do
```sh
new_column = flights.select((flights.air_time/60).alias("duration_hrs")) # new column is returned
```
Note that new_column is variable used for just assignment. Actual new name of the column in the dataframe will be given in alias(). Also, we can do the same operation with .withColumn()
```sh
flights = flights.withColumn("duration_hrs", flights.air_time/60) # dataframe with new column is returned
```
Pay attention to the return. The .withColumn() method returns dataframe with newly generated column.


The .select() equivalent Spark DataFrame method .selectExpr() takes SQL expressions as a string:
```sh
flights.selectExpr("air_time/60 as duration_hrs")
```
with the SQL as keyword being equivalent to the .alias() method. To select multiple columns, you can pass multiple strings.

### Aggregation:
All of the common aggregation methods, like .min(), .max(), and .count() are GroupedData methods. These are created by calling the .groupBy() DataFrame method. You'll learn exactly what that means in a few exercises. For now, all you have to do to use these functions is call that method on your DataFrame. For example, to find the minimum value of a column, col, in a DataFrame, df, you could do
```sh
df.groupBy().min("col").show()
```
This creates a GroupedData object (so you can use the .min() method), then finds the minimum value in col, and returns it as a DataFrame. Example:
```sh
Find the shortest flight from PDX in terms of distance:
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

Find the longest flight from SEA in terms of air time:
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()
```
Note that here operator e.g. min("colName") takes the column name inside the operator. 

### Grouping and Aggregating:
Part of what makes aggregating so powerful is the addition of groups. PySpark has a whole class devoted to grouped data frames: pyspark.sql.GroupedData, which you saw in the last two exercises.

You've learned how to create a grouped DataFrame by calling the .groupBy() method on a DataFrame with no arguments.

Now you'll see that when you pass the name of one or more columns in your DataFrame to the .groupBy() method, the aggregation methods behave like when you use a GROUP BY statement in a SQL query!
Example:
```sh
Group by tailnum:
by_plane = flights.groupBy("tailnum")

Number of flights each plane made:
by_plane.count().show()

Group by origin:
by_origin = flights.groupBy("origin")

Average duration of flights from PDX and SEA:
by_origin.avg("air_time").show()
```

In addition to the GroupedData methods you've already seen, there is also the .agg() method. This method lets you pass an aggregate column expression that uses any of the aggregate functions from the pyspark.sql.functions submodule.

This submodule contains many useful functions for computing things like standard deviations. All the aggregation functions in this submodule take the name of a column in a GroupedData table. Example:
```sh
Import functions:
import pyspark.sql.functions as F

Group by month and dest:
by_month_dest = flights.groupBy("month", "dest")

Average departure delay by month and destination:
by_month_dest.avg("dep_delay").show()

Standard deviation of departure delay:
by_month_dest.agg(F.stddev("dep_delay")).show()
```
I would prefer .agg() method since it can accept many functions that you maynot be able to pass directly to groupedData object.

### Joining:
In PySpark, joins are performed using the DataFrame method .join(). This method takes three arguments. 
```sh
Joined_dataframe = DataFrame1.join(DataFrame2,on="colname", how = "leftouter")
```

# Getting started with machine learning pipelines
We will focus on ML package. In practise, it is useful for now to use both packages as mllib has a lot more features than ML. However in the future these will be migrated to ML and mllib will be deprecated.

In short:
- ML: New, Pipelines, Dataframes, Easier to construct a practical machine learning pipeline
- MLlib: Old, RDD's, More features

At the core of the pyspark.ml module are the Transformer and Estimator classes. Almost every other class in the module behaves similarly to these two basic classes.

Transformer classes have a .transform() method that takes a DataFrame and returns a new DataFrame; usually the original one with a new column appended. For example, you might use the class Bucketizer to create discrete bins from a continuous feature or the class PCA to reduce the dimensionality of your dataset using principal component analysis.

Estimator classes all implement a .fit() method. These methods also take a DataFrame, but instead of returning another DataFrame they return a model object. This can be something like a StringIndexerModel for including categorical data saved as strings in your models, or a RandomForestModel that uses the random forest algorithm for classification or regression.

"label" is the default name for the response variable in Spark's machine learning routines.
We will go through basic functionality of PySpark ML, for little more advance functionality refer to this blog https://medium.com/@dhiraj.p.rai/logistic-regression-in-spark-ml-8a95b5f5434c

### Data Types
Before you get started modeling, it's important to know that Spark only handles numeric data. That means all of the columns in your DataFrame must be either integers or decimals (called 'doubles' in Spark).

When we imported our data, we let Spark guess what kind of information each column held. Unfortunately, Spark doesn't always guess right and you can see that some of the columns in our DataFrame are strings containing numbers as opposed to actual numeric values.

To remedy this, you can use the .cast() method in combination with the .withColumn() method. It's important to note that .cast() works on columns, while .withColumn() works on DataFrames.

The only argument you need to pass to .cast() is the kind of value you want to create, in string form. For example, to create integers, you'll pass the argument "integer" and for decimal numbers you'll use "double".

You can put this call to .cast() inside a call to .withColumn() to overwrite the already existing column, just like you did in the previous chapter!

### String to Integer
To convert the type of a column using the .cast() method, you can write code like this:
```sh
dataframe = dataframe.withColumn("col", dataframe.col.cast("new_type"))

Example: Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
```
Note that .withColumn() method returns the whole dataframe along with the specified column (unlike .select() method which returns only the specified column).

We will use model_data for modeling in the next sections.


### Strings and Factors
As you know, Spark requires numeric data for modeling. So far this hasn't been an issue; even boolean columns can easily be converted to integers without any trouble. But you'll also be using the airline and the plane's destination as features in your model. These are coded as strings and there isn't any obvious way to convert them to a numeric data type.

Fortunately, PySpark has functions for handling this built into the pyspark.ml.features submodule. You can create what are called 'one-hot vectors' to represent the carrier and the destination of each flight. A one-hot vector is a way of representing a categorical feature where every observation has a vector in which all elements are zero except for at most one element, which has a value of one (1).

Each element in the vector corresponds to a level of the feature, so it's possible to tell what the right level is by seeing which element of the vector is equal to one (1).

The first step to encoding your categorical feature is to create a StringIndexer. Members of this class are Estimator s that take a DataFrame with a column of strings and map each unique string to a number. Then, the Estimator returns a Transformer that takes a DataFrame, attaches the mapping to it as metadata, and returns a new DataFrame with a numeric column corresponding to the string column.

The second step is to encode this numeric column as a one-hot vector using a OneHotEncoder. This works exactly the same way as the StringIndexer by creating an Estimator and then a Transformer. The end result is a column that encodes your categorical feature as a vector that's suitable for machine learning routines!

This may seem complicated, but don't worry! All you have to remember is that you need to create a StringIndexer and a OneHotEncoder, and the Pipeline will take care of the rest.

```sh
Example: In this exercise you will create a StringIndexer and a OneHotEncoder to code the carrier column. To do this, you will call the class constructors with the arguments inputCol and outputCol. The inputCol is the name of the column you want to index or encode, and the outputCol is the name of the new column that the Transformer should create.

Create a StringIndexer:
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")

Create a OneHotEncoder:
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")
```
If you want to see the affect of these operations before going to Pipeline, then follow
```sh
This step will take the 'carrier' column in model_data and add 'carrier_index' column to the model_data and return
model_data = carr_indexer.fit(model_data).transform(model_data)

Next, this step will add 'carrier_fact' column to the model_data
model_data = carr_encoder.transform(model_data)

Looking at these three columns:
model_data.select('carrier','carrier_index','carrier_fact').show(5)
+-------+-------------+--------------+
|carrier|carrier_index|  carrier_fact|
+-------+-------------+--------------+
|     VX|          8.0|(10,[8],[1.0])|
|     AS|          0.0|(10,[0],[1.0])|
|     VX|          8.0|(10,[8],[1.0])|
|     WN|          1.0|(10,[1],[1.0])|
|     AS|          0.0|(10,[0],[1.0])|
+-------+-------------+--------------+
```
Note that HotEncoding in Spark is described differently than in Scikit-learn. It is to store the sparse information in compact form.

### Assemble a vector:
The last step in the Pipeline is to combine all of the columns containing our features into a single column. This has to be done before modeling can take place because every Spark modeling routine expects the data to be in this form. You can do this by storing each of the values from a column as an entry in a vector. Then, from the model point of view, every observation is a vector that contains all of the information about it and a label that tells the modeler what value that observation corresponds to.

Because of this, the pyspark.ml.feature submodule contains a class called VectorAssembler. This Transformer takes all of the columns you specify and combines them into a new vector column.
```sh
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")
```
If you want to see the transformed data then follow this 
```sh
model_data = vec_assembler.transform(model_data)
model_data.select("features").show(truncate=False)
```
otherwise, no need to use the transformation here as we will use the vec_assembler object directly in the Pipeline (in the next section). You can also involve the standarization step to the features vectors. Refer to Appendix section 2.

### Now create the Pipeline
Pipeline is a class in the pyspark.ml module that combines all the Estimators and Transformers that you've already created. This lets you reuse the same modeling process over and over again by wrapping it up in one simple object. 
```sh
Import Pipeline:
from pyspark.ml import Pipeline

Make the pipeline: Call the Pipeline() constructor with the keyword argument stages to create a Pipeline
flights_pipe = Pipeline(stages=[carr_indexer, carr_encoder, vec_assembler])
```

### Modeling Data Preparation
In Spark it's important to make sure you split the data after all the transformations. This is because operations like StringIndexer don't always produce the same index even when given the same list of strings.
```sh
Fit and transform the data:
piped_data = flights_pipe.fit(model_data).transform(model_data)
```

Here, model_data is the original data on which estimator and transformer are defined (in the pipeline) to preprocess few of the columns data. Now that you've done all your manipulations, the last step before modeling is to split the data!
```sh
Split the data into training and test sets:
training, test = piped_data.randomSplit([0.6, 0.4], seed = 5)
```
# Model Tuning and Selection
### Create the modeler
The Estimator you'll be using is a LogisticRegression from the pyspark.ml.classification submodule.

```sh
Import LogisticRegression:
from pyspark.ml.classification import LogisticRegression

Create a LogisticRegression Estimator:
lr = LogisticRegression()
```

### Cross validation | Create an evaluator
The first thing you need when doing cross validation for model selection is a way to compare different models. Luckily, the pyspark.ml.evaluation submodule has classes for evaluating different kinds of models. Your model is a binary classification model, so you'll be using the BinaryClassificationEvaluator from the pyspark.ml.evaluation module.

This evaluator calculates the area under the ROC. This is a metric that combines the two kinds of errors a binary classifier can make (false positives and false negatives) into a simple number. 
```sh
Import the evaluation submodule:
import pyspark.ml.evaluation as evals

Create a BinaryClassificationEvaluator:
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")
```
### Make a grid
Next, you need to create a grid of values to search over when looking for the optimal hyperparameters. The submodule pyspark.ml.tuning includes a class called ParamGridBuilder that does just that (maybe you're starting to notice a pattern here; PySpark has a submodule for just about everything!).

You'll need to use the .addGrid() and .build() methods to create a grid that you can use for cross validation. The .addGrid() method takes a model parameter (an attribute of the model Estimator, lr, that you created a few exercises ago) and a list of values that you want to try. The .build() method takes no arguments, it just returns the grid that you'll use later.

```sh
Import the tuning submodule:
import pyspark.ml.tuning as tune

Create the parameter grid:
grid = tune.ParamGridBuilder()

Add the hyperparameter:
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

Build the grid:
grid = grid.build()
```
### Make the validator
The submodule pyspark.ml.tuning also has a class called CrossValidator for performing cross validation. This Estimator takes the modeler you want to fit, the grid of hyperparameters you created, and the evaluator you want to use to compare your models.

```sh
Create the CrossValidator:
cv = tune.CrossValidator(estimator=lr,
                         estimatorParamMaps=grid,
                         evaluator=evaluator)
```
### Fit the model(s):
```sh
Fit cross validation models:
models = cv.fit(training)

Extract the best model:
best_lr = models.bestModel
```
Once you obtain the best model then, you can use this model for evaluation on train/test data

```sh
Use the model to predict the test data:
test_results = best_lr.transform(test)

Evaluate the predictions:
print(evaluator.evaluate(test_results))
```


# Appendix

### 1. To change the default spark configuration

Import the required classes:
```sh
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
```
Get the default configurations from spark as your existing SparkSession:
```sh
spark.sparkContext._conf.getAll()

You can also get fewer details with SparkConf:
SparkConf().getAll()
```
Update the default configurations:
```sh
conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
```
Stop the current Spark Session:
```sh
spark.sparkContext.stop()
```
Create the Spark Session:
```sh
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

### 2. Standard Scalar
Once we have feature vector from VectorAssembler, we can standardize the data and create a new feature column. We can also use this step in the Pipeline.
```sh
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler().setInputCol("features").setOutputCol("scaled_features")
```
This scaler can be passed in to Pipeline. Otherwise, we can directly transform the data and visualize -
```sh
model_data = scaler.fit(model_data).transform(model_data)
model_data.select("features","scaled_features").show()
```

### 3. Logistic Regression in PySpark ML: 
https://www.hadoopinrealworld.com/sparksession-vs-sparkcontext-vs-sqlcontext-vs-hivecontext/

This link shows 
- Handling imbalance data (a new column is generated to put more weightage to class with less data, this column can be specified in the classifier algorithm)
- Feature Selection using chisquareSelector:
```sh
from pyspark.ml.feature import ChiSqSelector
css = ChiSqSelector(featuresCol='Scaled_features',outputCol='Aspect',labelCol='Outcome',fpr=0.05)
train=css.fit(train).transform(train)
test=css.fit(test).transform(test)
test.select("Aspect").show(5,truncate=False)
```


### 4. Feature Engineering in PySpark ML: 
https://medium.com/@dhiraj.p.rai/essentials-of-feature-engineering-in-pyspark-part-i-76a57680a85



# Open Questions
- how to submit pyspark jobs
- 




