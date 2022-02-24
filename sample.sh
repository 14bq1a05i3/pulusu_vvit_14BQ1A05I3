curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
https://login.microsoftonline.com/f5222e6c-5fc6-48eb-8f03-73db18203b63/oauth2/v2.0/token \
-d 'client_id=c8807a23-7b1c-42ac-9c48-99686f967189' \
-d 'grant_type=client_credentials' \
-d 'scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d%2F.default' \
-d 'client_secret=GM0JY.DTkaNAnUy4g.Tw7t1wbVPN7hp-8a'
--output json
