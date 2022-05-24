# crowdTangleAnalisys

## Data Collection

According with the url files in URLs folder, the getLinks.py file was used to collect all the news mentioning that URL from the CSV file
The API used was "Links" (https://github.com/CrowdTangle/API/wiki/Links)

## Data Analisys

With all the data in MongoDB, the script readData.py could be used to start analise the data, it can be focused in
    Collections
    Accounts
These focus are used to create the graphs according with which node to group data

After the graph created, it's opened using Gephi (https://gephi.org/users/download) application

PS: Script populateDatabase.py can be used to populate Database using the .bson files