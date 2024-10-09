# StatsCleaner Sentinel Version
Command line tool for Clean Stats on Redis (3scale)

Run from command line

git clone https://github.com/dandan2000/StatsCleaner.git

cd StatsCleaner

mvn clean package

java -jar target/stats-cleaner-2.jar 90 127.0.0.1:26379,10.0.0.1:26379 2 mymaster pass1234 pass1111


where 90 is days, 127.0.0.1:26379,10.0.0.1:26379 is set of sentinel hosts divided per comma without any spaces, 2 is DB, mymaster is the name of group, pass1234 is password the master password and pass1111 is the sentinel's password

All stats older thar days will be deleted

password could be absent and set as a env with export sc_master_pass=mypass and sc_sentinel_pass=thepass
note that env var take precedence over command line password

Build Docker Image

export DOCKER_HOST=$(echo -n 'unix://' ; podman machine inspect --format {{.ConnectionInfo.PodmanSocket.Path}})

s2i build -U $DOCKER_HOST --ref sentinel https://github.com/dandan2000/StatsCleaner registry.access.redhat.com/ubi8/openjdk-17 quay.io/<USER>/statscleaner

Run Docker Image

podman run quay.io/dacelent/statscleaner java -jar /deployments/stats-cleaner-2.jar  90 127.0.0.1:26379,10.0.0.1:26379 2 mymaster pass1234 pass1111

