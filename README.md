## Big Data Management and Analytics

### Assignment1

#### Instructions to run the code

Clone the repo

```
git clone https://github.com/Pranavm/BigDataAssignment1
```

Create the jar File by running maven with goal package

```
mvn package
```

Now you can find the jar in the target folder. Copy the jar file to a machine where hadoop runs.

From a machine where hadoop is running run the following command to run the code


```
 hadoop jar BigDataAssignment1-0.0.1-SNAPSHOT.jar DownLoadFromWebToHadoop ${DIRECTORY} z {url1} {url2} ...
```

The package only contains the DownLoadFromWebToHadoop class. The class takes as its first argument the hadoop directory to store the downloaded files. This should be of the form hdfs://....  
Next argument is the type of the file to be downloaded. Specify "z" if the file type is an archive and you want to extract it. Any value other than see will be ignored.  
This can be followed by any number of urls to download. The urls should ideally points to archived files as the program tries to extract this file.