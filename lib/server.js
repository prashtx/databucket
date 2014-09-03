'use strict';

var bodyParser = require('body-parser');
var express = require('express');
var logger = require('morgan');

var config = require('./config');
var routes = require('./routes');


var app = express();

app.set('port', config.port);

app.use(logger('common'));

app.use(bodyParser.urlencoded({ extended: true }));

app.use(function (req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.use(routes);

app.listen(app.get('port'), function () {
  console.log('Listening on port ' + app.get('port'));
});
