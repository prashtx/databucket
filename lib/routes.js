'use strict';

var express = require('express');
var Factual = require('factual-api');
var Promise = require('bluebird');

var config = require('./config');

Promise.promisifyAll(Factual.prototype);

var factual = new Factual(config.factualKey, config.factualSecret);

var routes = module.exports = new express.Router();

// Returns the entries whose hours overlap the range on the specified day.
function filterByHours(data, day, range) {
  var out = [];
  var len = 0;
  var hours;
  var beg;
  var end;
  var i;
  for (i = 0; i < data.length; i += 1) {
    if (data[i].hours) {
      hours = data[i].hours[day];
    } else {
      hours = null;
    }
    if (hours) {
      // TODO: handle multiple ranges of hours
      // TODO: handle minutes
      beg = parseInt(hours[0][0].split(':')[0], 10);
      end = parseInt(hours[0][1].split(':')[0], 10);
      if ((beg < range[0] && range[0] < end) ||
          (beg < range[1] && range[1] < end) ||
          (beg > range[0] && end < range[1])) {
        out[len] = data[i];
        len += 1;
      }
    }
  }

  return out;
}

// GET /api/places/count/retail?day=monday&beg=12&end=14&bbox=0,1,2,3
// bbox is W,S,E,N
routes.get('/api/places/count/retail', function (req, res) {
  var bbox = req.query.bbox.split(',').map(parseFloat);
  var day = req.query.day;
  var range = [parseInt(req.query.beg, 10), parseInt(req.query.end, 10)];

  var total;

  function get(offset) {
    return factual.getAsync('/t/places-us', {
      offset: offset,
      limit: 50,
      include_count: true,
      geo: {
        $within: {
          $rect: [[bbox[3], bbox[2]], [bbox[1], bbox[0]]]
        }
      },
      filters: { category_ids: { $includes: 123 } }
    });
  }

  var offset = 0;
  function getMatches(offset, matches) {
    if (offset === undefined) {
      offset = 0;
    }

    if (matches === undefined) {
      matches = [];
    }

    return get(offset).spread(function (response) {
      if (total === undefined) {
        total = response.total_row_count;
      }

      offset += response.included_rows;

      var newMatches = filterByHours(response.data, day, range);
      var i;
      var len = matches.length;
      for (i = 0; i < newMatches.length; i += 1) {
        matches[len + i] = newMatches[i];
      }

      if (offset < total) {
        return getMatches(offset, matches);
      }

      return matches;
    });
  }


  getMatches().then(function (matches) {
    res.status(200).send({ count: matches.length });
  }).catch(function (error) {
    console.log(error);
    res.status(500).end();
  });
});
