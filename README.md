# HDFS-MAMI

HDFS-MAMI provides a REST-API to upload raw data into an HDFS while maintaining metadata in a MongoDB.

For more details please read the file `docs/DOC.md`.

## Uploading

Before you upload you need to sketch out how you want to arrange your data. If you have small data files
it's recommended that you upload them into a SequenceFile. If you have huge data files (+64MB) it's recommended
that you upload them as standalone/regular files. 

If you chose to upload your small files into a SequenceFile you have to sketch out which files you want to group
together. We use SequenceFiles as an archive format to store multiple files into a single file. You may for example
choose to store all data from a certain week into the same SequenceFile. 

### Metadata

If you upload a raw data file you have to provide metadata. This metadata has to be a JSON file.
Mandatory are the properties *msmntCampaign*, *format* and if you upload into a SequenceFile *seq*.

A metadata file for an upload (or multiple uploads) may look like this:

```json
{"msmntCampaign":"testing","format":"txt","seq":"0004"}
```

*seq* specifies into which SequenceFile the uploaded file should be stored in. All these properties
are limited to `a-zA-Z0-9\-` and can not be longer than 32 characters. The folder structure on the
server is `<msmntCampaign>/<format>/<fileName>` (in case of uploading into a SequenceFile *fileName* is
`<seq>.seq`).

What metadata must contain or should contain is up to the uploaders to decide. It is highly recommended
that you supply enough metadata to make searching raw data as easy as possible. Information that can be
extracted from the raw data directly must not be included in the metadata. 

### API-Keys

Since the service publicly reachable there's an access control mechanism. You need to send
your API-Key in each request as part of the request header `X-API-KEY`. You may obtain an API-Key
through *munt* or *sten*. 

If you suspect that your API-Key was stolen or leaked (API-Keys need to be kept secret) immediately do:

```
curl -X POST -H "X-API-KEY: key" https://217.150.246.7:6443/hdfs/mgmt/revoke --insecure
```

to revoke your key. Also immedatiely talk to *munt* or *sten*.

### Uploading with curl

You may upload raw data with curl or any other program and/or library you choose. 
The upload methods will return a SHA1 hash of the raw data received. You may check this hash against your local version of the raw data to
ensure that the data was received correctly. 

*Uploading into a SequenceFile*

```
$ cat small2.txt
this is two

$ cat small.meta
{"msmntCampaign":"testing","format":"txt","seq":"0003"}

$ curl -v -i -H "X-API-KEY: key" -F meta=@small.meta -F data=@small1.txt https://217.150.246.7:6443/hdfs/seq/up/small1.txt --insecure
ee4f42fa585d9bb88f25e83f6f8cea6563749585

$ curl -v -i -H "X-API-KEY: key" -F meta=@small.meta -F data=@small2.txt https://217.150.246.7:6443/hdfs/seq/up/small2.txt --insecure
baa29d3adbe3e71d99cb00473c75f5c0f14818d7
```

List files and download:

```
$ curl -v -i -H "X-API-KEY: key" https://217.150.246.7:6443/hdfs/fs/seq/ls/testing/txt/0003.seq --insecure
["small1.txt","small2.txt"]

$ curl -v -i -H "X-API-KEY: key" https://217.150.246.7:6443/hdfs/fs/seq/raw/testing/txt/0003.seq?fileName=small2.txt --insecure
this is two
```

*Uploading standalone file*

```
$ cat numbers.csv 
1;2;3
4;5;6

$ cat numbers.meta
{"msmntCampaign":"testing", "format":"csv"}

$ curl -v -i -H "X-API-KEY: key" -F meta=@numbers.meta -F data=@numbers.csv https://217.150.246.7:6443/hdfs/up/numbers.csv --insecure
8faed7e2e8f5e39f8939fe8f873201d3f58b28ee

$ sha1sum numbers.csv 
8faed7e2e8f5e39f8939fe8f873201d3f58b28ee  numbers.csv
```

List files and download:

```
$ curl  -H "X-API-KEY: key"  https://217.150.246.7:6443/hdfs/fs/ls/testing/csv/ --insecure
["numbers.csv"]

$ curl  -H "X-API-KEY: key"  https://217.150.246.7:6443/hdfs/fs/raw/testing/csv/numbers.csv --insecure
1;2;3
4;5;6
```
