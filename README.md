# Data-Intensive Computing Project Proposal: 
# Time Series Prediciton on NYC caps 

## Group: 
**Dream_Team_v2**
- **Xiya Sun**
- **Silvia Pasini**
- **Lijie Li**
- **Davis Siemens**

## Institution
- **KTH**  
- **Course**: Data Intensive Computing  
- **Date**: 13th September 2025  

## Goal Of Project 
Develop a big data pipeline to time series forecast NYC yellow taxi demand using scalable storage, processing, and machine learning tools. We will feature engineer the target variable to be demand per hour for a given location.  


## Dataset
The dataset can be found on Kaggle ([https://www.kaggle.com/elemento/nyc-yellow-taxi-trip-datale](https://www.kaggle.com/elemento/nyc-yellow-taxi-trip-data)). 
It contains information about NYC Yellow Taxi trips, including pickup and dropoff locations, timestamps, and trip distances, for the months Jan 2015, Jan 2016, Feb 2016 & March 2016.

### Features in the Dataset
<table border="1">
	<tr>
		<th>Field Name</th>
		<th>Description</th>
	</tr>
	<tr>
		<td>VendorID</td>
		<td>
		A code indicating the TPEP provider that provided the record.
		<ol>
			<li>Creative Mobile Technologies</li>
			<li>VeriFone Inc.</li>
		</ol>
		</td>
	</tr>
	<tr>
		<td>tpep_pickup_datetime</td>
		<td>The date and time when the meter was engaged.</td>
	</tr>
	<tr>
		<td>tpep_dropoff_datetime</td>
		<td>The date and time when the meter was disengaged.</td>
	</tr>
	<tr>
		<td>Passenger_count</td>
		<td>The number of passengers in the vehicle. This is a driver-entered value.</td>
	</tr>
	<tr>
		<td>Trip_distance</td>
		<td>The elapsed trip distance in miles reported by the taximeter.</td>
	</tr>
	<tr>
		<td>Pickup_longitude</td>
		<td>Longitude where the meter was engaged.</td>
	</tr>
	<tr>
		<td>Pickup_latitude</td>
		<td>Latitude where the meter was engaged.</td>
	</tr>
	<tr>
		<td>RateCodeID</td>
		<td>The final rate code in effect at the end of the trip.
		<ol>
			<li> Standard rate </li>
			<li> JFK </li>
			<li> Newark </li>
			<li> Nassau or Westchester</li>
			<li> Negotiated fare </li>
			<li> Group ride</li>
		</ol>
		</td>
	</tr>
	<tr>
		<td>Store_and_fwd_flag</td>
		<td>This flag indicates whether the trip record was held in vehicle memory before sending to the vendor,<br> aka “store and forward,” because the vehicle did not have a connection to the server.
		<br>Y= store and forward trip
		<br>N= not a store and forward trip
		</td>
	</tr>
	<tr>
		<td>Dropoff_longitude</td>
		<td>Longitude where the meter was disengaged.</td>
	</tr>
	<tr>
		<td>Dropoff_ latitude</td>
		<td>Latitude where the meter was disengaged.</td>
	</tr>
	<tr>
		<td>Payment_type</td>
		<td>A numeric code signifying how the passenger paid for the trip.
		<ol>
			<li> Credit card </li>
			<li> Cash </li>
			<li> No charge </li>
			<li> Dispute</li>
			<li> Unknown </li>
			<li> Voided trip</li>
		</ol>
		</td>
	</tr>
	<tr>
		<td>Fare_amount</td>
		<td>The time-and-distance fare calculated by the meter.</td>
	</tr>
	<tr>
		<td>Extra</td>
		<td>Miscellaneous extras and surcharges. Currently, this only includes. the $0.50 and $1 rush hour and overnight charges.</td>
	</tr>
	<tr>
		<td>MTA_tax</td>
		<td>0.50 MTA tax that is automatically triggered based on the metered rate in use.</td>
	</tr>
	<tr>
		<td>Improvement_surcharge</td>
		<td>0.30 improvement surcharge assessed trips at the flag drop. the improvement surcharge began being levied in 2015.</td>
	</tr>
	<tr>
		<td>Tip_amount</td>
		<td>Tip amount – This field is automatically populated for credit card tips.Cash tips are not included.</td>
	</tr>
	<tr>
		<td>Tolls_amount</td>
		<td>Total amount of all tolls paid in trip.</td>
	</tr>
	<tr>
		<td>Total_amount</td>
		<td>The total amount charged to passengers. Does not include cash tips.</td>
	</tr>
</table>

## Tools & Metholodolgy
We propose the following pipeline. 

 **HDFS:** Store raw NYC Yellow Taxi CSV files for distributed access.  
- **PySpark:** Clean data, filter outliers, extract time/location features, and aggregate demand.  
- **Pyspark ML** Train scalable regression models with lag and calendar features.  
- **Apache Cassandra:** Save forecasts keyed by zone and timestamp for fast retrieval.  
- **Matplotlib or Plotly:** Plot actual vs. predicted demand curves for evaluation.   

## Presentation of Work 

- The coding project will be published in [Github](https://github.com/dasiemens-coder/NYC-yellow-cab.git) and made publicly available after submission deadline. 
- Additionally, the project and report will be uploaded on Canvas according to the guidelines. 