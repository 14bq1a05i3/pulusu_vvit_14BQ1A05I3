curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
https://login.microsoftonline.com/${{ secrets.AZURE_CREDENTIALS.tenantId }}/oauth2/v2.0/token \
-d 'client_id=${{ secrets.AZURE_CREDENTIALS.clientId }}' \
-d 'grant_type=client_credentials' \
-d 'scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d%2F.default' \
-d 'client_secret=${{ secrets.AZURE_CREDENTIALS.clientSecret }}'
