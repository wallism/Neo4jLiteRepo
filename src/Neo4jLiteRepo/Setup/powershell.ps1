
# on first creation of the DB, AUTH needs to be set to none!   -e NEO4J_AUTH=none `
# then run: ALTER USER neo4j SET PASSWORD 'thiswillbechanged' `;
# then delete the container
# remove  -e NEO4J_AUTH=none from the docker run (now the pwd is set in the DB it is not necessary in the docker run)
# docker run again  `
# open http://localhost:7474 in a new tab.
# then log in with the password you set above 
# - it will then ask for a CHANGE password (have to do this once, and before connecting from code) 

# auth OFF (new DB)
# -e NEO4J_AUTH=none `

$now = Get-Date -Format "yyyyMMdd"
$product = "neo4jlite"
docker run -d --rm `
  --name neo4j-$product-$now `
  -e server.memory.heap.initial_size=1G `
  -e server.memory.heap.max_size=4G `
  -e server.memory.pagecache.size=2G `
  -v C:\Projects\_Mark\Neo4jLiteRepo\volumedata-${product}:/data `
  -p 7474:7474 `
  -p 7687:7687 `
  --memory="7g" `
  neo4j:latest

  # make sure you have in your .gitignore: volumedata/
