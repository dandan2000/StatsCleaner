# StatsCleaner
Command line tool for Clean Stats on Redis (3scale)

Run from command line

git clone https://github.com/dandan2000/StatsCleaner.git

cd StatsCleaner

mvn clean package

java -jar target/stats-cleaner-1.jar 90 192.168.1.152 6379 2 mypass


where 90 is days, 192.168.1.152 is host, 6379 is port, 2 is DB, mypass is password

All stats older thar days will be deleted

password could be absent and set as a env with export sc_pass=mypass
note that env var take precedence over command line password

Build Docker Image

export DOCKER_HOST=$(echo -n 'unix://' ; podman machine inspect --format {{.ConnectionInfo.PodmanSocket.Path}})

s2i build -U $DOCKER_HOST https://github.com/dandan2000/StatsCleaner registry.access.redhat.com/ubi8/openjdk-17 quay.io/<USER>/statscleaner

Run Docker Image

podman run quay.io/dacelent/statscleaner java -jar /deployments/stats-cleaner-1.jar  90 192.168.1.152 6379 2 pass1234


