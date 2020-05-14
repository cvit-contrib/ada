
var levels = {
    gpu: {
        0: { n: 0,  mins:0 },
        1: { n: 1, mins: 600},
        2: { n: 2, mins: 2400},
        3: { n: 4, mins: 6000},
        4: { n: 4, mins: 12000},
        5: { n: 4, mins: 24000},
        6: { n: 4, mins: 32000},
        7: { n: 8, mins: 64000}
    },

    next : function(level){
        level = level + 1;
        return this.gpu[level];
    },

    previous : function(level){
        level = level - 1;
        return this.gpu[level];

    },

    level: function(datum){
        for(var x=1; x <= 6; x++){
            if(datum.GrpTRESMins < this.gpu[x].mins){
                return x-1;
            }
        }
        return -1;
    },

    upgrade : function(datum){
        if ( datum.fusage > 90 ){
            var level = this.level(datum);
            var next = this.next(level);
            return [true, datum, next, "#upgrade"];
        }
        else if (datum.fusage < 25 ){
            var level = this.level(datum);
            var previous = this.previous(level);
            return [true, datum, previous, "#downgrade"];

        }
        return [false, datum, this.level];
    }
}

var tabulate = function (data,columns) {
    var table = d3.select('#usage-table').append('table')
    table.attr("class", "table table-striped table-sm");
    var thead = table.append('thead')
    var tbody = table.append('tbody')

    thead.append('tr')
        .selectAll('th')
        .data(columns)
        .enter()
        .append('th')
        .text(function (d) { return d })

    var rows = tbody.selectAll('tr')
        .data(data)
        .enter()
        .append('tr')

    var cells = rows.selectAll('td')
        .data(function(row) {
            return columns.map(function (column) {
                return { column: column, value: row[column] }
            })
        })
        .enter()
        .append('td')
        .text(function (d) { return d.value })

    return table;
}

var display_levels = function(){
    var data = d3.entries(levels.gpu)
    var rows = d3.select("#level-desc")
        .selectAll("tr")
        .data(data)
        .enter()
        .append("tr");


    rows.selectAll("td")
        .data(d => [d.key, d.value.n, d.value.mins])
        .enter()
        .append('td')
        .text(d => d);


}

display_levels();


d3.csv('data.csv',function (data) {
    var changes = [];
    data.forEach (function(d){
        d.GrpTRESMins = +d.GrpTRESMins;
        d.GrpTRES = +d.GrpTRES;
        d.GPUUsage = +d.GPUUsage;
        d.fusage = Math.round(100*(d.GPUUsage/d.GrpTRESMins));

        var decision = levels.upgrade(d);
        if(decision[0]){
            changes.push(decision.slice(1, decision.length));
        }

    });

    var cmds = [];
    changes.forEach ( function(d) {
        var user = d[0], delta = d[1], remark = d[2];
        if(delta){
            var es = [
                "modquota", 
                user.Account, 
                delta.n, 
                delta.mins, 
                remark,
                user.fusage];
            cmds.push(es.join(' '));
        }
    });


    var script = cmds.join('\n');

    var updates = d3.select("#updates-new").text(script);


    data.sort((x, y) => d3.descending(x.fusage, y.fusage));
    var columns = ['Account', 'GrpTRES', 'GrpTRESMins', 'GPUUsage', 'fusage']
    tabulate(data,columns)

    var used = d3.sum(data, d => d.GPUUsage);
    var worst_case = d3.sum(data, d => d.GrpTRESMins);
    var total = 60*7*24*60;
    total_stats([used, worst_case-used, total-worst_case], 
        ["used", "unused", "unalloc"]);

})

var total_stats = function(data, tags){
    var metadata = []
    var total = d3.sum(data);

    var psum = 0;
    for(var i=0; i < data.length; i++){
        var obj = {
            "width": data[i],
            "x": psum,
            "total": total,
            "label": tags[i]
        };
        metadata.push(obj);
        psum += data[i];
    }

    var width = 640, height = 60;

    var chart = d3.select("#total-usage").append('svg')
        .attr("class", "chart")
        .attr("width", width) // bar has a fixed width
        .attr("height", height);


    var x = d3.scaleLinear() // takes the fixed width and creates the percentage from the data values
        .domain([0, total])
        .range([0, width]);

    var color = d3.scaleOrdinal(d3.schemePastel2);


    chart.selectAll("rect") // this is what actually creates the bars
        .data(metadata)
        .enter().append("rect")
        .attr("width", d => x(d.width))
        .attr("height", height)
        .attr("fill", (d, i) => color(i))
        .attr("transform", d => "translate(" + x(d.x) + ", 0)");

    var format = function(w){
        var f = w.width/w.total;
        return d3.format(".1%")(f);
    }

    var pgroup = chart.append("g");
    pgroup.selectAll("text") // adding the text labels to the bar
        .data(metadata)
        .enter().append("text")
        .attr("x", d => x(d.x + d.width/2))
        .attr("y", height/3) // y position of the text inside bar
        .attr("dx", -3) // padding-right
        .attr("dy", ".35em") // vertical-align: middle
        .attr("text-anchor", "middle") // text-align: right
        .text(format);

    var tgroup = chart.append("g");
    tgroup.selectAll("text") // adding the text labels to the bar
        .data(metadata)
        .enter().append("text")
        .attr("x", d => x(d.x + d.width/2))
        .attr("y", 2*height/3) // y position of the text inside bar
        .attr("dx", -3) // padding-right
        .attr("dy", ".35em") // vertical-align: middle
        .attr("text-anchor", "middle") // text-align: right
        .attr("font-size", "12") 
        .text(d => d.label);

}

