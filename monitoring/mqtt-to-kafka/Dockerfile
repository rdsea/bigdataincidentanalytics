FROM node:10

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY src .
EXPOSE 8080
CMD [ "node", "index.js" ]

# The following 2 RUN commands are necessary for the correct installation of node-rdkafka
# See https://github.com/Blizzard/node-rdkafka/blob/master/examples/docker-alpine.md
#RUN apk --no-cache add \
#      bash \
#      g++ \
#      ca-certificates \
#      lz4-dev \
#      musl-dev \
#      cyrus-sasl-dev \
#      openssl-dev \
#      make \
#      python
#
#RUN apk add --no-cache --virtual .build-deps gcc zlib-dev libc-dev bsd-compat-headers py-setuptools bash

#RUN mkdir -p /usr/local/app
#RUN mkdir -p /usr/local/app/src
#COPY src /usr/local/app/src/
#COPY package.json /usr/local/app

#RUN mkdir /mqtt-to-kafka
#RUN mkdir /mqtt-to-kafka/src
#COPY src /mqtt-to-kafka/src/
#COPY package.json /mqtt-to-kafka
# COPY . /mqtt-to-kafka
#WORKDIR /usr/local/app
#
#RUN npm install
#CMD ["node", "src/index.js"]
