[Create some folders for persistence]
mkdir /opt/docker/nexus /opt/docker/nexus/certs /opt/docker/rth

[Place some dummy certs]
cd /opt/docker/nexus/certs
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 3650

[Launch rethink for service]
docker run -d -p 18080:8080 -p 38015:28015 --name rth -v "/opt/docker/rth/data:/data" rethinkdb

[Launch nexus]
docker run -d -p 18081:8080 -p 10080:80 -p 10443:443 -p 11717:1717 -p 11718:1718 --name nexus -v "/opt/docker/nexus/data:/data" -v "/opt/docker/nexus/certs:/certs" nayarsystems/nexus -l http://0.0.0.0:80 -l https://0.0.0.0:443 -l tcp://0.0.0.0:1717 -l ssl://0.0.0.0:1718 --sslCert /certs/nexus.crt --sslKey /certs/nexus.key

[Available ports are]
18080 -> RethinkDB Web of service
38015 -> RethinkDB client port of service
18081 -> RethinkDB Web of nexus
10080 -> Nexus HTTP/WS
10443 -> Nexus HTTPS/WSS
11717 -> Nexus TCP
11718 -> Nexus SSL
