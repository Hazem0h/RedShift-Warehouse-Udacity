


## Issues I faced
* We must define attributes in the same order as they appear in the JSON file when creating the events staging table. This is because we told the COPY command what schema to use. Otherwise, attributes will not be assigned correctly, and errors will occur
* We must specify the timeformat in the COPY command to be milliseconds. Otherwise the timestamp will not be parsed successfully
* Some attributes have long strings that can go out of range of a default `text` attribute. For that, use `VARCHAR(MAX)`
* The song data in S3 is distributed across lots of files. So copying over the entire song data is very time consuming, esepcially with a small cluster. When experimenting, while using one node for example, limit the files you're copying. For example, limit only songs in the `/A/A/A` directory in the COPY command. 
