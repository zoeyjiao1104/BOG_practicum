# pipeline

Files for accessing and processing data using available APIs and web scraping. Detects anomalous gear events and generates predictions for gear location and temperature. Data is persisted to a database when a pipeline job is executed.

## analysis

Scripts for generating predictions, detecting anomalies, and bounding buoys' range of motion.

## data

Storage space for pipeline data files.

## jobs

Scripts used to orchestrate the collection and merging of BOG buoy data and oceanographic datasets, as well as modeling of the data.

## load

Scripts for loading data from external sources into a database through POST requests.

## retrieval

Scripts for fetching data from external sources (APIs, webscraping, etc.) and then reshaping into a standardized format. Each data source is represented as a `MeasurementRetrieval` subclass.

## utilities

Modules/functions/classes used across the repository.
