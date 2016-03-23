Date: 2016-03-16

*Currently running at:  https://217.150.246.7:6443/hdfs/*

# MAMI HDFS REST WebService

## Examples

```
$ cat small2.txt
this is two

$ cat small.meta
{"msmntCampaign":"testing","format":"txt","seq":"0003"}

$ curl -v -i -H "X-API-KEY: key" -F meta=@small.meta -F data=@small1.txt https://217.150.246.7:6443/hdfs/seq/up/small1.txt --insecure
ee4f42fa585d9bb88f25e83f6f8cea6563749585

$ curl -v -i -H "X-API-KEY: key" -F meta=@small.meta -F data=@small2.txt https://217.150.246.7:6443/hdfs/seq/up/small2.txt --insecure
baa29d3adbe3e71d99cb00473c75f5c0f14818d7

$ curl -v -i -H "X-API-KEY: key" https://217.150.246.7:6443/hdfs/fs/seq/ls/testing/txt/0003.seq --insecure
["small1.txt","small2.txt"]

$ curl -v -i -H "X-API-KEY: key" https://217.150.246.7:6443/hdfs/fs/seq/raw/testing/txt/0003.seq?fileName=small2.txt --insecure
this is two
```


## Configuration

The config file will be loaded from `/etc/hdfs-mami/srvc.cfg` per default. 
You can override that path by setting the `MAMI_HDFS_CFG_PATH` java system property. 

### Settings

An example configuration might look like this:

```
URL=http://localhost:9998/
HDFS_PATH=hdfs://localhost:9000/test
AUTH_DB_NAME=auth
UPLOAD_DB_NAME=uploads
```

#### AUTH_DB_NAME

Name of the database used to store authentication information. 

### LOG_DB_NAME

Name of the database used to store the access log.

### HDFS_PATH

Path to the HDFS file system. 

#### UPLOAD_DB_NAME

Name of the database used to store upload information.

### URL

The URL the REST service will be run at (this also defines the port it will listen on).

## AuthDB

The AuthDB (auth database) contains the API-keys and their associated access level.
The schema is:

```q
{"_id":<id>,
 "api_key":<api_key>,
 "access_level":<access_level>,
 "name":<name>}
```

Uses the collection ```api_keys``` in the *AUTH_DB_NAME* database. *name* is the (short-)name of
the person the API-Key belongs to. *access_level* is an integer value with flags set: 

```
read = 1
write = 2
read/write = 1 OR 2 = 3
admin = 4
```

## UploadDB

The UploadDB (upload database) contains an entry for every uploaded file (or partially upoladed file). 
The schema is:

```q
{"_id":<id>,
 "path":<path of the file>, 
 "complete":<boolean>,
 "sha1":<sha1 hash of file contents>,
 "meta":<metadata>,
 "uploader":<name of the uploader,
 "seqKey":<seqKey>,
 "timestamp":<time of upload (unix timestamp)>}
```

*metadata* refers to the metadata specified by the uploader of the file when calling the *Upload Raw Data* REST-method. It may be an arbitrary JSON document but it includes at least ```msmntCampaign``` and ```format``` (see *Upload Raw Data* REST-method).
An API-Key has a name associated which is stored as *uploader* in each entry in the collection. *seqKey* is used when the file is placed in a SequenceFile. 

Uses the collection ```uploads``` in the *UPLOAD_DB_NAME* database.

The collection ```upload_errors`` in the UploadDB is used for debugging. If upload fails an entry is created. The schema is:

```
{"path":<path>,
 "seqKey":<seqKey>,
 "msg":<errmsg>,
 "timestamp":<time of error (unix timestamp>)}
```

### SequenceFiles

If the file was uploaded as part of a SequenceFile then the *path* will point to the sequence file and 
*seqKey* will contain the key of the entry in the SequenceFile. 

## LogDB

The access log database (LogDB) stores the access log. The access log records all uploads and file system accesses. The schema is:

```q
{"path":<path>,
 "action":<action>,
 "name",
 "timestamp":<timestamp>}
```

*action* refers to what has been done (i.e. "rm" for removing a file).

## Access levels

Several different access levels exist. These are:

* none: Dummy access level. No special permissions.
* read: Permission to read files and list directory contents.
* write: Permission to upload new raw data.
* admin: Permission to delete, rename and create files.

## API-Keys

Much of the functionality requires an API-Key that has to be sent within the request in the
```X-API-KEY``` header. An API-Key has an associated access level. API-Keys
are to be kept private, must not be shared. If you think that your API-Key is not secret anymore please revoke the key by calling the *Revoke API-Key* REST-method. An API-Key
has a name associated. This name refers to the person responsible for the API-Key. 
The name is mainly used for logging purposes as due to security reasons the API-Key
should not be logged. 

## API

## Error Codes

401 is used when the API-Key is invalid or the associated access level is not enough
to use a REST-method. 500 is used when an internal error has occured. If you see
a 500 error please notify the hoster of the REST-service.

### Filesystem Operations

### Download Binary File (Huge File)

```GET /fs/bin/{path}```
```q
Path Parameters:
 - path: Path of the file.
Returns: application/octet-stream
```

Download a binary file. Requires *read* permissions.

### Download Text File (Huge File)

```GET /fs/raw/{path}```
```q
Path Parameters:
 - path: Path of the file.
Returns: text/plain
```

Download a text file. Requires *read* permissions.

### Download Binary File (SequenceFile)

```GET /fs/seq/bin/{path}?fileName={fileName}```
```q
Path Parameters:
 - path: Path to the SequenceFile.
Query Parameters:
 - fileName: Name of the small file inside the SequenceFile.
Returns: application/octet-stream
```

Download a binary file. Requires *read* permissions.

### Download Text File (SequenceFile)

```GET /fs/seq/raw/{path}?fileName={fileName}```
```q
Path Parameters:
 - path: Path to the SequenceFile.
Query Parameters:
 - fileName: Name of the small file inside the SequenceFile.
Returns: text/plain
```

Download a text file. Requires *read* permissions.

### List Files

```GET /fs/ls/{path}```
```q
Path Parameters:
 - path: Path of the directory.
Returns: application/json
```

Lists all files in a directory and returns an array of the file names.
Requires *read* permissions.

### List Files (SequenceFile)

```GET /fs/seq/ls/{path}```
```q
Path Parameters:
 - path: Path of the SequenceFile.
Returns: application/json
```

Lists all files in the SequenceFile and returns an array of the file names.
Requires *read* permissions.

### List Files Recursive

```GET /fs/lsR/{path}```
```q
Path Parameters:
  - path: Path of the directory.
Returns: application/json
```

Lists all files in a directory recursively and returns an array of paths (**not** file names). Requires *read* permissions.

### Status

```GET /status```

If this does not respond with a status code of ```200 OK``` then the service is not running. 

### Upload Raw Data

**Note:** As of now uploading huge files using this method will not work as
they will be loaded into memory first. We're hoping to fix this. 

```POST /fs/seq/up/{fileName}```
```q
Path Parameters:
  - fileName: Name of the file (including extension).
              This will be used as the key for the entry in the
              SequenceFile.
Accepts: multipart/form-data
Form Data Parameters:
  - meta: Metadata associated with the file.
  - data: raw data
```

*meta* must be valid JSON and must contain ```msmntCampaign (String)```,
```format (String)``` and ```seq (String)```. 
```seq``` is an identifier (such as for example ```0000```) which identifies
the SequenceFile to put the new data into. 

The SequenceFile used will be ```WHDFS_PATH + '/' + msmntCampaign + '/' + format + '/' + seq + '.seq'``` and
it will be created if neccessary. 

Upon initiating an upload an entry in the *upload* database will be created with the flag
```complete``` set to ```false```. After completing the upload the database entry will be updated and ```complete``` will be set to ```true```. A SHA1 hash of the uploaded data will be stored in the *upload* database as well.

Requires *write* permissions.

#### About SequenceFiles

A SequenceFile is an archive of small files. This is due to HDFS not liking too many small files.
A SequenceFile is a Key-Value file format. 


### Upload Raw Data (Huge Files)

**Important:** Should be used for huge files (>=1GB) only and only after discussion. 

**Note**: If you want to just test uploading use *testing* for *msmntCampaign* (```{"msmntCampaign":"testing"}```). 

```POST /up/{fileName}```
```q
Path Parameters:
  - fileName: Name of the file (including extension)
Accepts: multipart/form-data
Form Data Parameters:
  - meta: Metadata associated with the file. 
  - data: raw data.
```
*meta* must be valid JSON and must contain ```msmntCampaign (String)``` and
```format (String)``` as for example ```{"msmntCampaign" : "cmp000", "format" : "csv-foo"}```.

Upon initiating an upload an entry in the *upload* database will be created with the flag
```complete``` set to ```false```. After completing the upload the database entry will be updated and ```complete``` will be set to ```true```. A SHA1 hash of the uploaded data will be stored in the *upload* database as well.

The file will be stored under ```WHDFS_PATH + '/' + msmntCampaign + '/' + format + '/' fileName```. 

Requires *write* permissions.

### Management

#### Revoke API-Key

```POST /mgmt/revoke```

Revokes the API-Key that was sent within the request. Use this if you think that somebody knows your API-Key who shouldn't know it. 

```GET /mgmt/accessLevel```

Returns the access level the API-Key sent within the request grants. 
