# Week 10: PySpark Data Processing Summary Report

## Introduction
This report summarizes the data processing project for Week 10, which utilizes PySpark for analyzing a large dataset. The project includes data transformations and Spark SQL queries to derive insights from the data.

## PySpark Script
Below is an outline of the PySpark script used for data processing:

```python
# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("Week10DataProcessing").getOrCreate()

# Load the large dataset (replace 'your_dataset_path' with the actual path)
data = spark.read.csv("movies.csv", header=True, inferSchema=True)

# Data Transformation Example:
transformed_data = data.select("Film", "Genre", "Worldwide Gross").filter(col("Audience score %") > 50)

# Spark SQL Query Example:
data.createOrReplaceTempView("data_table")
sql_result = spark.sql("SELECT Year, AVG(Profitability) AS AvgProfitability FROM data_table GROUP BY Year")

# Save the transformed data and SQL result if needed
transformed_data.write.csv("transformed", header=True, mode="overwrite")
sql_result.write.csv("sql_result", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
```

## Data Processing Functionality
The PySpark script successfully loads the dataset, performs data transformations, and executes a Spark SQL query. The data transformations involve selecting specific columns and filtering based on audience score. These operations enhance the data for further analysis.

## Use of Spark SQL and Transformations
1. **Spark SQL**: A Spark SQL query is used to calculate the average profitability of movies by year, providing valuable insights into the dataset.

2. **Data Transformations**: Data transformations are performed to focus on relevant columns and filter data based on audience score, ensuring that only pertinent information is retained.

## Deliverables
- [PySpark Script](main.py)
### Transformed Data
- Transformed Data: The transformed data is saved in the "transformed" directory in CSV format.

It looks like this:
| Film                                    | Genre    | Worldwide Gross |
|-----------------------------------------|----------|-----------------|
| Zack and Miri Make a Porno             | Romance  | $41.94          |
| Youth in Revolt                         | Comedy   | $19.62          |
| What Happens in Vegas                   | Comedy   | $219.37         |
| Water For Elephants                     | Drama    | $117.09         |
| WALL-E                                  | Animation| $521.28         |
| Waitress                                | Romance  | $22.18          |
| Waiting For Forever                     | Romance  | $0.03           |
| Valentine's Day                         | Comedy   | $217.57         |
| Twilight: Breaking Dawn                 | Romance  | $702.17         |
| Twilight                                | Romance  | $376.66         |
| The Ugly Truth                          | Comedy   | $205.30         |
| The Twilight Saga: New Moon            | Drama    | $709.82         |
| The Time Traveler's Wife               | Drama    | $101.33         |
| The Proposal                            | Comedy   | $314.70         |
| The Duchess                             | Drama    | $43.31          |
| The Curious Case of Benjamin Button     | Fantasy  | $285.43         |
| Tangled                                 | Animation| $355.01         |
| She's Out of My League                  | Comedy   | $48.81          |
| Sex and the City                        | Comedy   | $415.25         |
| Remember Me                            | Drama    | $55.86          |
| Rachel Getting Married                  | Drama    | $16.61          |
| Penelope                                | Comedy   | $20.74          |
| P.S. I Love You                         | Romance  | $153.09         |
| One Day                                 | Romance  | $55.24          |
| Not Easily Broken                       | Drama    | $10.70          |
| No Reservations                         | Comedy   | $92.60          |
| Nick and Norah's Infinite Playlist     | Comedy   | $33.53          |
| My Week with Marilyn                    | Drama    | $8.26           |
| Music and Lyrics                        | Romance  | $145.90         |
| Miss Pettigrew Lives for a Day          | Comedy   | $15.17          |
| Midnight in Paris                       | Romance  | $148.66         |
| Marley and Me                           | Comedy   | $206.07         |
| Mamma Mia!                              | Comedy   | $609.47         |
| Mamma Mia!                              | Comedy   | $609.47         |
| Made of Honor                           | Comedy   | $105.96         |
| Love & Other Drugs                     | Comedy   | $54.53          |
| Life as We Know It                     | Comedy   | $96.16          |
| License to Wed                         | Comedy   | $69.31          |
| Letters to Juliet                       | Comedy   | $79.18          |
| Knocked Up                              | Comedy   | $219            |
| Just Wright                             | Comedy   | $21.57          |
| Jane Eyre                               | Romance  | $30.15          |
| It's Complicated                        | Comedy   | $224.60         |
| I Love You Phillip Morris              | Comedy   | $20.10          |
| High School Musical 3: Senior Year     | Comedy   | $252.04         |
| He's Just Not That Into You            | Comedy   | $178.84         |
| Good Luck Chuck                         | Comedy   | $59.19          |
| Going the Distance                      | Comedy   | $42.05          |
| Gnomeo and Juliet                      | Animation| $193.97         |
| Gnomeo and Juliet                      | Animation| $193.97         |
| Four Christmases                        | Comedy   | $161.83         |
| Fireproof                               | Drama    | $33.47          |
| Enchanted                               | Comedy   | $340.49         |
| Dear John                               | Drama    | $114.97         |
| Beginners                               | Comedy   | $14.31          |
| Across the Universe                     | Romance  | $29.37          |
| A Serious Man                           | Drama    | $30.68          |
| A Dangerous Method                      | Drama    | $8.97           |
| 27 Dresses                              | Comedy   | $160.31         |
| (500) Days of Summer                   | Comedy   | $60.72          |
### SQL Result
- SQL Result: The result of the Spark SQL query is saved in the "sql_result" directory in CSV format.
It looks like this:

| Year | AvgProfitability  |
|------|-------------------|
| 2007 | 4.0585            |
| 2009 | 5.0233            |
| 2010 | 2.0376            |
| 2011 | 3.1899            |
| 2008 | 8.1913            |


## Conclusion
This project demonstrates the use of PySpark for efficient data processing on a large dataset. By incorporating Spark SQL queries and data transformations, valuable insights can be derived from the data. The resulting transformed data and SQL results are available for further analysis and reporting.



