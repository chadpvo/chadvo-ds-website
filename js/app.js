// 1. Select the div
const container = d3.select("#chart-area");

// 2. Append an SVG canvas
const svg = container.append("svg")
    .attr("width", 400)
    .attr("height", 400)
    .style("border", "1px solid black");

// 3. Draw a circle
svg.append("circle")
    .attr("cx", 200)      // Center X coordinate
    .attr("cy", 200)      // Center Y coordinate
    .attr("r", 80)        // Radius
    .attr("fill", "steelblue")
    .on("mouseover", function() {
        d3.select(this).attr("fill", "orange"); // Interaction test
    })
    .on("mouseout", function() {
        d3.select(this).attr("fill", "steelblue");
    });