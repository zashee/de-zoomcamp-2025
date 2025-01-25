# Homework 2
## Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
```postgresql
SELECT COUNT(1)
FROM yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020-%.csv';
```

## Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
```postgresql
SELECT COUNT(1)
FROM green_tripdata
WHERE filename LIKE 'green_tripdata_2020-%.csv';
```

## Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
```postgresql
SELECT COUNT(1)
FROM yellow_tripdata
WHERE filename = 'yellow_tripdata_2021-03.csv';
```