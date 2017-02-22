FROM alpine 

RUN apk update && apk add ca-certificates netcat-openbsd
COPY rethink_persist.alpine /rethink_persist
COPY schemas /schemas

ENTRYPOINT ["/rethink_persist"]