var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res) {

    var db = req.db;
    var query = [
       {$unwind: "$entries"}, 
       {$group: {
               _id:"$pattern", 
               entries: {$push: "$entries"}, 
               size:{$sum:1}
           }
       }, 
       {$match: { size: { $gt: 2 } }},
       {$sort:{size:-1}}
    ];
    db.pattern404.aggregate(query, function(err, docs) {
        res.render('index', { content: docs });
    });
});

module.exports = router;
