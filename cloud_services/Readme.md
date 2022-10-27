### Gen 3 Cloud Services

### Release History

#### version 0.0.1

Supported Cloud Providers and Services
1. AWS
    1. S3
    2. Dynamo DB

##### API Interface Details
Required parameters required for any cloud service provider
<li>cloud_provider_name: options are aws, gcp or azure</li>

Required parameters required for all AWS Services
<ul>
<li>region</li>
<li>service_name</li>
<li>profile</li>
</ul>

Service Specific parameters required for all AWS Services

#### S3 Actions
 <table>
 <th>
 Action Name
 </th>
  <th>
 Action Description
 </th>
 <th>
 Required Inputs
 </th>
 <th>
 Output and data type
 </th>
 <tr>
 <td>
 get_buckets
 </td>
  <td>
 returns recursively the list of all buckets in an account
 </td>
 <td>
 None
 </td>
  <td>
 get_buckets_response
 <ul>
     <li>status - True or False
     <li>bucket_count - integer
     <li>bucket_list - list
 </ul> 
 </td>
 </tr>

 <tr>
 <td>
 get_object_list
 </td>
  <td>
 returns recursively the list of all objects in a bucket
 </td>
 <td>
 bucket_name
 </td>
  <td>
 get_object_list_response
 <ul>
     <li>status - True or False
     <li>bucket_count - integer
     <li>bucket_contents - list
 </ul> 
 </td>
 </tr>

<tr>
 <td>
 get_prefix_objects
 </td>
  <td>
 returns recursively the list of all objects in a bucket
 </td>
 <td>
 bucket_name
 </td>
  <td>
get_prefix_objects_response
 <ul>
     <li>status - True or False
     <li>bucket_count - integer
     <li>bucket_contents - list
 </ul> 
 </td>
 </tr>

 <tr>
 <td>
 get_objects_in_prefix
 </td>
  <td>
 returns recursively the list of all objects in a prefix
 </td>
 <td>
 bucket_name
 prefix_name
 </td>
  <td>
 get_objects_in_prefix_response
 <ul>
     <li>status - True or False
     <li>files - list of all files in the bucket - list
     <li>file_count - total files in the bucket - integer
     <li>folders - list of all prefixes in the bucket - list
     <li>folder_count - total prefix list in the bucket - integer
 </ul> 
 </td>
 </tr>

<tr>
 <td>
 download_object
 </td>
  <td>
 download a file from s3 to local
 </td>
 <td>
 bucket_name - Bucket Name where the file that needs to be downloaded exist
 prefix_name - Full Prefix path where to download file exists
 </td>
  <td>
 download_object_response
 <ul>
     <li>status - True or False
 </ul> 
 </td>
 </tr>

 <tr>
 <td>
 upload_object
 </td>
  <td>
 upload a local file to given bucket & prefix in S3
 </td>
 <td>
 bucket_name - Bucket Name where the file needs to be uploaded
 prefix_name - Full Prefix path where to upload the file
 local_file_name - Name of the local file that you need to upload
 delete_flag - True or False to delete the local file from the local computer
 </td>
  <td>
 upload_object_response
 <ul>
     <li>status - True or False
     <li>bucket_count - integer
     <li>bucket_contents - list
 </ul> 
 </td>
 </tr>

 <tr>
 <td>
 read_s3_file
 </td>
  <td>
 Reads the data in the given S3 file
 </td>
 <td>
 bucket_name - Bucket Name where the file that needs to be read exist
 prefix_name - Full path of the file to read
 file_type - any one of the following based on the file type - [csv, text, txt, json, parquet]
 </td>
  <td>
 read_s3_file_response
 <ul>
     <li>status - True or False
     <li>bucket_count - integer
     <li>bucket_contents - list
 </ul> 
 </td>
 </tr>

 </table>
 