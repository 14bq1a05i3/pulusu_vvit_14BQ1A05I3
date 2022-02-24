curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
https://login.microsoftonline.com/f5222e6c-5fc6-48eb-8f03-73db18203b63/oauth2/token \
-d 'client_id=<client-id>' \
-d 'grant_type=client_credentials' \
-d 'resource=https%3A%2F%2Fmanagement.core.windows.net%2F' \
-d 'client_secret=c8807a23-7b1c-42ac-9c48-99686f967189'
