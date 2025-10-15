This repo is a basic bare-bone code for the task that has
1. inputfile1 (File1) for the parquet file storing the detection events, storing the key fields : 
   - geo-location-id, identifier for geo-location 
   - item-name, which ia the object-type as defined in WOG Video Analytics metadata standards of one of these types [LUGGAGE, BACKPACK, TRASHBIN, TROLLEY, BICYCLE]
   - timestamp in milliseconds for the detection event, e.g. from System.currentTimeMillis()
   - detection-id, unique id for a particular detection event.  There can be duplicates as the assumption is that the detection algo used to generate the detection events are able to do item-tracking within and across cameras (cross-camera tracking)
   - sensor-id, unique id for the video camera which the detection event was captured
2. inputfile2 (File2) for the parquet file storing the reference table mapping for geo-location, i.e. its globally unique identifier to its string-description
Refer to the Events and GeoLoc class respectively for the exact field name used in the code
3. process to do display the top N items detected in each geo-location, generating a new column called rank, when a value of 1 means the top (1st) rank. When there is a tie for the rank e.g. both are rank number 1, both items will be displayed for the geo-location.
4. storing the results (geo-location-id, item-rank, item-name) in output.parquet files
Note that as geo-location-id (based on the design that it is a BigInt-compatible storage) is stored, there is no need to use the reference table (File2)

Getting Started
git clone https://github.com/SHSworking/SAssign.git

Scala version 2 and Sparks version 3 are used. Please refer to build.sbt for the details

Things to note:
There is a settings.config file which is used to define
1. FILE1 - detection events dataset, example ../input/file1event.parquet
2. FILE2 - reference table dataset, example "../input/file2reftable.parquet"
3. OUTPUT_FILE - output parquet file, example "../output/output.parquet"
4. TOP_N_ROW returns the top ranked row of the result, rank =1 means highest count
Please note that 
   (i) tie-breakers are included, i.e. if TOP_N_ROW=2, but there are two 2nd ranked items, both rows are returned
   (ii) to have no filter set TOP_N_ROW to 0 or negative value

As requested there are the following file :
1. Considerations.txt, for the considerations and assumptions 
2. joins.pdf, for the join strategies
3. design.pdf, for the design and considerations for Task 2

This project is developed in Intellj IDE, with build.sbt specifying the dependencies, 
 the Spark version and java version

________________________
Running the application
________________________
Run the MainAppl object's main , if you have the input files for the detection-events and geo-location
which loads the files and TOP_N_ROW from the settings file described above.

Otherwise, it is recommended to Run the MainTest object's main 
which generates the input files and output files, hardcoding to the /input and /output folders, and returning the top 2 ranked item per geo-location (TOP_N_ROW is set as 2)
The results is also display via console-terminal using println, showing the time thst elapsed since start of processing.
