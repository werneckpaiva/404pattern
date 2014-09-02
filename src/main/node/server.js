var http = require('http'); 
var db = require("mongojs").connect("mysite", ["pattern404"]);
http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/html'});
  
   
   var query = [
           {$unwind: "$entries"}, 
           {$group: {
                   _id:"$pattern", 
                   entries: {$push: "$entries"}, 
                   size:{$sum:1}
               }
           }, 
           {$sort:{size:-1}}
       ];
   
   
   
  db.pattern404.aggregate(query, function(err, patterns) {
    var content='<html><head><title>404 pattern</title>' +
                '<style>body{font-family: arial;} .pattern{cursor: pointer}</style>' +
                '<script src="http://code.jquery.com/jquery-1.11.1.min.js"></script>' +
                '</head>' +
                '<body>';
    if( err || !patterns) {
        content += "No patterns found";
        return;
    }
    var count=0;
    patterns.forEach( function(obj) {
        count++;
        content += "<div class=\"pattern\" onclick=\"$('#pattern_"+count+"').toggle()\">("+obj.entries.length+") "+obj._id+"</div>";
        content += "<ul id=\"pattern_"+count+"\" style=\"display:none\">";
        for (var i=0; i<obj.entries.length; i++){
            content += "<li>"+obj.entries[i].request+"</li>";
        }
        content += "</ul>";
    });
    content +='</body></html>\n';
    res.end(content);
  });
}).listen(9000, "127.0.0.1");
console.log('Server running at http://127.0.0.1:9000/');
