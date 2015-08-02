Access Integration Tool :-

Invoke url "https://3-dot-local-amenities.appspot.com/" to use the tool and tranform the csv files added to the cloud storage into entities in the data models of "Local Amenities Map".


Limitations :- 

1. This tool should be invoked after the csv files are successfully uploaded to the Google cloud storage by using upload tool.

2. The tool will identify and process only the csv files available in the Google cloud storage default bucket of app "local-amenities".

3. The tool cannot tranform the csv lines into data models for more than 5000 lines and will display "Deadline exceeded error". Hence while using upload tool please split your csv files into several files with 5000 lines in each and add to the app's directory and then upload to cloud storage.


