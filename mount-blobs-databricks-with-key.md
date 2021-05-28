https://docs.microsoft.com/en-us/azure/databricks/scenarios/store-secrets-azure-key-vault


```python
storage_name= "storagename"
container_name= "conatainer name"
scope_name= "db-scope"
key_name="key-value=secret-key"

config_key = "fs.azure.account.key." + storage_name + ".blob.core.windows.net"

dbutils.fs.mount(
source = "wasbs://%s@%s.blob.core.windows.net" % (container_name, storage_name),
mount_point = "/mnt/ecommerce",
extra_configs = {config_key:dbutils.secrets.get(scope = scope_name, key = key_name)})

dbutils.fs.ls("/mnt/ecommerce")
```
