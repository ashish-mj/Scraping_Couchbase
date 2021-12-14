import requests
from bs4 import BeautifulSoup
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator


def couchbase_connection():
    # Connection to cluster
    cluster = Cluster('couchbase://localhost', ClusterOptions(
  PasswordAuthenticator('Administrator', 'password')))
    # Connection to bucket
    bucket = cluster.bucket('Premier_League')
    # Connection to collection
    collection = bucket.default_collection()
    return collection


def scrape():
    url = 'https://www.sportskeeda.com/go/epl/standings'
    # make connection couchbase
    collection = couchbase_connection()
    req = requests.get(url)
    soup = BeautifulSoup(req.text,"html.parser")
    contents = soup.find('table',class_='keeda_points_table')
    
    trs = contents.find_all('tr')
    for tr in trs[1:]:
        tds = tr.find_all('td')
        row = [td.text.replace('\n', '') for td in tds]
        
        document ={"Team":row[1],"Matches Played":row[2],
                   "Won":row[3],"Draw":row[4],"Lost":row[5],
                   "Goal Difference":row[6],"Points":row[7]}
        # Insert document to couchbase colection
        collection.insert(row[0],document)

if __name__=="__main__":
    scrape()
    

        
        
        
