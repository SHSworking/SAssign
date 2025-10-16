_________
TITLE 
_________
This repo stores a basic code for processing input parquet file containing detection events, and generates an output parquet file that stores the top Nth-ranked items detected in each geo-location.

___________
OVERVIEW
___________
The overview and further details of the processing done are shown below.  It has the following:
1. inputfile1 (File1) for the parquet file storing the detection events with columns below : 
   - geo-location-id, identifier for geo-location 
   - item-name, which ia the object-type as defined in WOG Video Analytics metadata standards of one of these types [LUGGAGE, BACKPACK, TRASHBIN, TROLLEY, BICYCLE]
   - timestamp in milliseconds for the detection event, e.g. from System.currentTimeMillis()
   - detection-id, unique id for a particular detection event.  There can be duplicates as the assumption is that the detection algo used to generate the detection events are able to do item-tracking within and across cameras (cross-camera tracking)
   - sensor-id, unique id for the video camera which the detection event was captured
2. inputfile2 (File2) for the parquet file storing the reference table mapping for geo-location, i.e. its globally unique identifier to its string-description.
 Refer to the Events and GeoLoc class respectively for the exact field name used in the code
3. a processing to get the top N-ranked items detected in each geo-location, generating a new column called rank.  A value of 1 means the top (1st) rank. When there is a tie for the rank e.g. both are rank number 1, both items will be displayed for the geo-location.
4. storing the results in output.parquet file with columns : 
   - geographical_location,  BigInt-compatible
   - item_rank,  varchar(500)-compatible
   - item_name,  varchar(5000)-compatible.
 Note that as geographical_location stores the geo-location-id (based on the design that it is a BigInt-compatible storage being stored), there is no need to use the reference table (File2) to get the string-description

________________
Getting Started
________________
git clone https://github.com/SHSworking/SAssign.git

Scala version 2 and Spark version 3 are used. Please refer to build.sbt for the details

Things to note:
Please note that the setting file name is defined and fixed as "settings.config".
   '#' denotes start of a commented line TOP_N_ROW value must be an Integer.  The FILE1, FILE2, OUTPUT_FILE values are Strings, i.e. to the right of equality sign, the values can be enclosed within quotes or no quotes (if there are no spaces in between the value string).
This settings.config file is used to define 4 key-value pairs below:
1. FILE1 - detection events dataset, example ../input/file1event.parquet
2. FILE2 - reference table dataset, example "../input/file2reftable.parquet"
3. OUTPUT_FILE - output parquet file, example "../output/output.parquet"
4. TOP_N_ROW returns the top N-ranked row of the result, rank =1 refers to 1st ranked, i.e. highest count of detection.
Please note that 
   (i) tie-breakers are included, i.e. if TOP_N_ROW=1, and there are two 1st ranked items, both rows are returned.
   (ii) to have no filter, please set TOP_N_ROW to 0 or negative value.
To recap : TOP_N_ROW value must be an Integer, while the FILE1, FILE2, OUTPUT_FILE value are Strings

______________________
ADDITIONAL FILES/INFO
______________________
As requested, there are the following files :
1. Considerations.txt, for the considerations and assumptions 
2. joins.pdf, for the join strategies
3. design.pdf, for the design and considerations for Task 2

This project is developed in IntelliJ IDE, with build.sbt specifying the dependencies, 
  Spark version and Java version.

________________________
Running the application
________________________
Run the MainAppl object's main, if you have the input files (for the detection-events and geo-location reference table),
which loads the files and TOP_N_ROW from the settings file described above.
This assumes that you have :
1. detection events parquet file with columns below :
    - geographical_location_oid,   bigInt-compatible
    - video_camera_oid,   bigInt-compatible
    - detection_oid,   bigInt-compatible
    - item_name,   varchar(5000)-compatible
    - timestamp_detected,   bigInt-compatible
3. geo-locaation reference table with columns below :
    - geographical_location_oid,   bigInt-compatible
    - geographical_location,  varchar(500)-compatible

Otherwise, it is recommended to Run the MainTest object's main, 
which generates the input files, hardcoding to the /input and /output folders, and returning the top 2nd-ranked item per geo-location (TOP_N_ROW is set as 2).
The detection-event data generated contains 12 rows, of which 2 rows are exact duplicates (to simulate streaming ingestion 'at-least-once' processing - potential duplicate during ingestion).

With the sample input files generated, running MainAppl with settings.config's TOP_N_ROW=1, it test that both equally 1st-ranked item are selected. When TOP_N_ROW=2, it selected the top 2(rank 1st & 2nd) rows for the third geo-location.
 Please edit the testData1 variable in SimulateFiles class for more rows if you require. 
 The results is also displayed via console-terminal using println, showing the time thst elapsed since start of processing.
