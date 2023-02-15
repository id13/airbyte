# How to deploy

- Install helm v3
- Make sure you have access to GKE clusters and setup kubectl to be using context ia-jobs-cluster
- File in charts/values.production.yaml is used for the airbyte cluster
- Make sure the secret `airbyte-db` exists
- You need to have a service account file with admin access to Google Cloud Storage nearby (named service-account.json further) 
- execute the command `helm upgrade --debug --install -f values.production.yaml --set-string global.logs.gcs.credentialsJson="$(cat service-account.json | base64 -w 0)" airbyte airbyte/airbyte  --namespace=airbyte`