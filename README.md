# AdFrequencyViewer

The purpose of this project is to determine the user count with high frequency ads viewed more than 5-times.

Use case:

REQUIREMENT

Data science suspects that advertising campaigns are showing the same ad to users too many times (a high frequency) as they browse their favorite websites. They’ve asked the data engineers to investigate. Given two input files (ad_data.1.log and ad_data.2.log) containing tab delimited ad event data, find all of the users that saw the same ad more than 5x on a site.

POINTS TO BE TAKEN INTO CONSIDERATION:

Each line in the input files represents one user’s view of an ad on a site.
GUID is a unique identifier for a user
Filter out any ad events that do not have a valid GUID (i.e. GUID is “unsupported”, “-”, etc). To be valid, the GUID should be in a standard UUID format. Example: 310183a5-2a76-4742-a2f7-52c5faa605d5.
Output should be Ad ID, Site ID, Frequency and Total users that saw the ad at that frequency. Frequency is defined as the total number of times the same ad was shown to a user on the same site.
The output should be tab separated and sorted in descending order by frequency. The output should be consolidated into a single file.
EXAMPLE OUTPUT:

Ad ID	Site ID	Frequency	Total users that saw this ad at this frequency
Ad1	cnn	48	1
Ad2	wsj	25	5
Ad3	abc	10	20
Ad1	cnn	6	37
Meaning only 1 user saw Ad1 48 times on cnn. Five users saw Ad2 25x on wsj, and 20 users saw Ad3 on abc 10x. 37 users saw Ad1 on cnn 6 times.

The solution should be coded in Scala.

SOLUTION APPROACH:

The scala spark project AdFrequencyViewer provides solution to the above requirement in the following way:

Parse the input log files using spark
Filter out invalid GUIDs [GUIDs that dont comply with UUID standards] using the filter method.
Get the count as Frequency based on Ad ID, Site ID and GUID
Get the count as total_user based on Ad ID, Site ID and Frequency.
Write the output to a tab delimited file.
USAGE:

Below command is used to trigger this job in spark 2.x:

spark2-submit --class <class-name> jar-file <input_file_path> <output_file_path> 
example: spark2-submit --class com.nielsen.adviewer.AdFrequencyViewer adviewer-1.0-SNAPSHOT.jar "Input file path" "Output file path"
TECH STACK USED:

This scala spark project uses below software versions:

Scala 2.12.8
Spark 2.4.2
