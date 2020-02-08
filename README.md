# InsightProject
Data Engineering Insight Project
<h1>
Solar Insight
</h1>


<p>This web app allows you to get an insight of the solar electricity productions over the usa by zipcodes, as well as an realtime estimation of the current production.</p>

<p>The user to look over the maps of the usa to see the cloud coverage and the solar electricity production at a chosen date. He can also dive into the data for specific zipcode to analyse the electricity production history.</p>

<h1>
Design.
</h1>

<p>The historical data has been processed using Apache Spark. Satellite images are downloaded from a S3 database, matched to zipcode. For each zipcode-image pair, the cloud coverage over the location is extracted and then stored in a MySQL database. A web app implemented with Plotly Dash is then queriing this database to compute the electricity production.</p>

<p>The realtime data is first received by Apache Pulsar, and then fetched and processed similarly as for the historical data pipeline.</p>

<h1>
Repository Structure
</h1>

<h1>
AWS Setup
</h1>
<a href="/biby/InsightProject/blob/master/setup_spark.md">Here</a> is the detailled spark cluster set up 
<h1>
Data sets
</h1>

<p>I am using two main data sets:</p>
<ul>
<li>The tracking-the-sun dataset containing the list of solar panels attached to the grid: <a href="https://emp.lbl.gov/tracking-the-sun/" >here</a></li>
<li> The landsat8 dataset of satellite images (<a href="https://registry.opendata.aws/landsat-8/">here</a>). I am using specifically the Band 9 of the images.</li>
<li> A small zipcode dataset containing latitude, longitude and area.</li>
</ul>

