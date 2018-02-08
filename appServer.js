const uuid = require('uuid');
const http = require('http');
const hash = require('hash');
const fs = require('fs');
const config = require('./config');

let sessionStorage = {};

let svr = new http.Server(8088, [(v) => {
}, {}]);