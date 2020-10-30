solr-8.6.2
redis-2.8.41

# Import data
# _version_ field needs to be removed from input dataset
curl "http://klevu1.cust.oi.dev:8983/solr/collection1/update?commit=true&tr=updateXml.xsl" -H "Content-Type: text/xml" --data-binary @data.xml

# Queries
http://klevu1.cust.oi.dev:8983/solr/select?q=*:*&defType=edismax&bf=redis(id,test)&debugQuery=on
http://klevu1.cust.oi.dev:8983/solr/select?q=*:*&defType=edismax&bf=redis(id,core4:jackets)&debugQuery=on

# Start Solr on klevu1.cust.oi.dev
systems@klevu1.cust:~/solr-4.8.0/example$ mv /home/systems/solr-redis-0.1.jar lib/; java -jar start.jar | tee /tmp/solr.log

# Redis cmds
/home/systems/redis-2.8.4/src/redis-cli zadd test 200 1324-1313 
/home/systems/redis-2.8.4/src/redis-cli zrange test 0 -1 WITHSCORES

# Install local jar (un-mavenized)
mvn install:install-file -Dfile=ns-configuration.jar -DgroupId=ns -DartifactId=conf -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true

# Sematext plugin notes
- Redis hashmap keys should be IDs only, no cats
- query-time sort not possible
- only matches that are in redis will be returned by solr