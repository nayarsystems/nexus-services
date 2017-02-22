docker run --rm -v $GOPATH:/go golang:1.7-alpine sh -c "cd /go/src/github.com/nayarsystems/nexus-services/rethink-persist && go build -o rethink_persist.alpine"

cp "config_$1.json" config.json
docker build -t rethink_persist .

rm -f config.json
rm -f rethink_persist.alpine