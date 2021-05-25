```python
storageAccountName = "<<storage-name>>"
containerName = "<<container-name>>"

sas = "<<sas-token>>"

config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

print ('config key ', config)
# Blob Service

# mount the blob storage into databricsk DBFS - DataBricks File System
dbutils.fs.mount(
  source = "wasbs://"+containerName+"@"+storageAccountName+".blob.core.windows.net",
  mount_point = "/mnt/movielens",
  extra_configs = {config: sas} ) # config is key, value is sas token

```


```
# this ls command, actualyl query Azure blob over blob api
dbutils.fs.ls("/mnt/movielens")

```

```
# both "dbfs:///mnt/movielens" and "/mnt/movielens" are same
dbutils.fs.ls("dbfs:///mnt/movielens")
```

```
# DON't DO THIS , this WILL UNMOUNT 

#dbutils.fs.unmount("/mnt/movielens")
```
