// set the dimensions and margins of the graph
const width = 600;
height = 600
margin = 40


// The radius of the pieplot is half the width or half the height (smallest one). I subtract a bit of margin.
const radius = Math.min(width, height) / 2 - margin;

// append the svg object to the div called 'my_dataviz'
const svg = d3.select("#instrumentClasses")
    .append("svg")
    .attr("width", width)
    .attr("height", height)
    .append("g")
    .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

// Create dummy data
const xhReq = new XMLHttpRequest();
xhReq.open("GET", "/dashboard/instrumentClassesWithCount", false);
xhReq.send(null);
const data = JSON.parse(xhReq.responseText);

// set the color scale
const color = d3.scaleOrdinal()
    .domain(data)
    .range(d3.schemeSet2);

// Compute the position of each group on the pie:
const pie = d3.pie()
    .value(function (d) {
        return d.value;
    });
const data_ready = pie(d3.entries(data));
// Now I know that group A goes from 0 degrees to x degrees and so on.

// shape helper to build arcs:
const arcGenerator = d3.arc()
    .innerRadius(0)
    .outerRadius(radius);

// Build the pie chart: Basically, each part of the pie is a path that we build using the arc function.
svg
    .selectAll('mySlices')
    .data(data_ready)
    .enter()
    .append('path')
    .attr('d', arcGenerator)
    .attr('fill', function (d) {
        return (color(d.data.key))
    })
    .attr("stroke", "black")
    .style("stroke-width", "2px")
    .style("opacity", 0.7)

// Now add the annotation. Use the centroid method to get the best coordinates
svg
    .selectAll('mySlices')
    .data(data_ready)
    .enter()
    .append('text')
    .text(function (d) {
        return d.data.key + " Count: " + d.data.value
    })
    .attr("transform", function (d) {
        return "translate(" + arcGenerator.centroid(d) + ")";
    })
    .style("text-anchor", "middle")
    .style("font-size", 17)
