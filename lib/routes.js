'use strict';

var _ = require('lodash');
var express = require('express');
var Factual = require('factual-api');
var Promise = require('bluebird');
var request = require('request');

var config = require('./config');

Promise.promisifyAll(Factual.prototype);
Promise.promisifyAll(request);

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

function getAllFactual(query, filter) {
  var total;

  function get(offset) {
    var q = _.defaults({
      offset: offset,
      limit: 50,
      include_count: true
    }, query);
    return factual.getAsync('/t/places-us', q);
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
        //total = response.total_row_count;
        total = Math.min(500, response.total_row_count);
      }

      offset += response.included_rows;

      var newMatches;
      if (filter) {
        newMatches = filter(response.data);
      } else {
        newMatches = response.data;
      }

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

  return getMatches();
}


// GET /api/places.geojson?category=123&bbox=0,1,2,3
// bbox is W,S,E,N
routes.get('/api/places.geojson', function (req, res) {
  var bbox = req.query.bbox.split(',').map(parseFloat);
  var category = parseInt(req.query.category, 10);

  getAllFactual({
      geo: {
        $rect: [[bbox[3], bbox[0]], [bbox[1], bbox[2]]]
      },
      filters: { category_ids: { $includes: category } }
  }, function filter(data) {
    return data.map(function (item) {
      return {
        type: 'Feature',
        id: item.factual_id,
        properties: {
          address: item.address,
          name: item.name
        },
        geometry: {
          type: 'Point',
          coordinates: [item.longitude, item.latitude]
        }
      };
    });
  }).then(function (features) {
    res.status(200).send({
      type: 'FeatureCollection',
      features: features
    });
  }).catch(function (error) {
    console.log(error);
    res.status(500).end();
  });
});

// GET /api/places/count/retail?day=monday&beg=12&end=14&bbox=0,1,2,3
// bbox is W,S,E,N
routes.get('/api/places/count/retail', function (req, res) {
  var bbox = req.query.bbox.split(',').map(parseFloat);
  var day = req.query.day;
  var range = [parseInt(req.query.beg, 10), parseInt(req.query.end, 10)];

  getAllFactual({
      geo: {
        $within: {
          $rect: [[bbox[3], bbox[2]], [bbox[1], bbox[0]]]
        }
      },
      filters: { category_ids: { $includes: 123 } }
  }, function filter(data) {
    return filterByHours(data, day, range);
  }).then(function (matches) {
    res.status(200).send({ count: matches.length });
  }).catch(function (error) {
    console.log(error);
    res.status(500).end();
  });
});

// GET /api/photos.csv?lat=32&lng=-122
routes.get('/api/photos.csv', function (req, res) {
  var now = Date.now();
  //var period = 5*24*60*60; // 5 days in seconds
  var period = 6*60*60; // 6 hours in seconds
  var sets = 4 * 28; // 4 weeks

  function getWindow(beg, end) {
    var total = [];
    var prevMin;
    function getChunk(a, b) {
      var count = 200;
      var q = {
        lat: req.query.lat,
        lng: req.query.lng,
        distance: req.query.distance,
        client_id: config.igID,
        access_token: config.igToken,
        min_timestamp: a,
        max_timestamp: b,
        count: count
      };
      return request.getAsync({
        url: 'https://api.instagram.com/v1/media/search',
        qs: q
      }).spread(function (response, body) {
        var parsed = JSON.parse(body);
        console.log(_.template('${a} : ${b} -- ${count}', {
          a: a,
          b: b,
          count: parsed.data.length
        }));
        var chunk = _.map(parsed.data, function (item) {
          var ts = parseInt(item.created_time, 10);
          var date = new Date(ts * 1000);
          return {
            ts: ts,
            lat: item.location.latitude,
            lng: item.location.longitude,
            day: date.getDay(),
            hour: date.getHours()
          };
        });

        var len = total.length;
        var i;
        for (i = 0; i < chunk.length; i += 1) {
          total[len + i] = chunk[i];
        }

        var finished = false;
        if (chunk.length <= 1) {
          finished = true;
        }

        var min;
        if (!finished) {
          min = _.min(chunk, 'ts').ts;
          if (min === prevMin) {
            finished = true;
          } else {
            prevMin = min;
          }
        }

        if (finished) {
          console.log('Done with a window');
          return total;
        }
        return getChunk(beg, min - 1);
      });
    }

    return getChunk(beg, end);
  }

  var windows = [];
  var i;
  var stop = Math.floor(Date.now() / 1000);
  var start = stop - period + 1;

  for (i = 0; i < sets; i += 1) {
    windows[i] = {
      start: start - i*period,
      stop: stop - i*period
    };
  }

  res.status(200);
  res.write('lat,lng,ts,day,hour\n');
  Promise.map(windows, function (w) {
    return getWindow(w.start, w.stop)
    .then(function (set) {
      set.forEach(function (item) {
        item.ts = (new Date(item.ts * 1000)).toISOString();
        res.write(_.template('${lat},${lng},${ts},${day},${hour}\n', item));
      });
    });
  }, {
    concurrency: 10
  }).then(function () {
    res.end();
  }).catch(function (error) {
    console.log(error);
    console.log(error.stack);
    res.status(500).send();
  });
});
