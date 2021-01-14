const Schedule = require('node-schedule');
const Fs = require('fs');
const Crypto = require("crypto");
const Moment = require('moment');
Moment.locale('id');
const Dotenv = require('dotenv');
Dotenv.config();
let DataManager = require('./Includes/DataManager.js');
const REDIS_PORT = process.env.REDIS_PORT
//End imports

let dm = new DataManager(REDIS_PORT, function(){
    //dm.deleteBlacklist("test");
    //dm.cleanSlate()
    dm.deleteRedundancies();
    //dm.disconnect();
});