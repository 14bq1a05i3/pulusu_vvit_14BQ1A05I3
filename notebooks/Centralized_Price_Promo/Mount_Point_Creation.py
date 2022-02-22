# Databricks notebook source
def mounting(mount_point, source, clientId, keyVaultName):
  configs= {"fs.azure.account.auth.type":"OAuth",
	   "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
	   "fs.azure.account.oauth2.client.id":clientId,
	   "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="scopeKeyVault",key=keyVaultName),
	   "fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/a6b169f1-592b-4329-8f33-8db8903003c7/oauth2/token"
       }
  if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    pass
  else:
    dbutils.fs.mount(
            source= source,
            mount_point=mount_point,
            extra_configs = configs)

# COMMAND ----------

# mount_point = '/mnt/centralized-price-promo'
# source = 'abfss://centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/'
# clientId = ''
# keyVaultName = ''



# mounting(mount_point, source, clientId, keyVaultName)