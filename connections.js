const elasticsearch = require("elasticsearch");
const dotenv = require('dotenv');
dotenv.config();

let fdsConfig = {
  host: process.env.FDS_HOST,
  requestTimeout: 60000000,
  levels: "error"
};

module.exports.fds = new elasticsearch.Client(fdsConfig);
