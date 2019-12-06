"use strict";

const Bridge = require("mqtt-to-kafka-bridge");

let config = require("./config.js");

const bridge = new Bridge(config);

bridge.on("error", (error) => { console.log(error) });
bridge
    .run()
    .catch((error) => { console.log(error) });