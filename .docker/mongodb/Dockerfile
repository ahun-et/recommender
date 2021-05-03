
# build version
ARG MONGO_VERSION

FROM mongo:${MONGO_VERSION}

ENTRYPOINT [ "/usr/bin/mongod", "--bind_ip_all", "--replSet", "rs01" ]