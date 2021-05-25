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
