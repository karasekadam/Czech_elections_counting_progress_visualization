// size managing variables
const width = 960;
const height = 600;

// variables for election and map data
let okresCiselnik = {};
let results = {};
let finalVotersNumber = {};
let countedAt = {};
let partyResults = {};
let totalVoters = 0;
let countedAtGlobal = {};

// waves managing variables
let currWave = 1;
const minWave = 1;
const maxWave = 75;

// Color scale for the map
const colorScale = d3.scalePow()
    .exponent(0.4)
  .domain([0, 100])
  .range(["white", "darkblue"]);


function loadData() {
    // Loading the final voters number
    d3.csv("./okres_total_voters.csv").then(data => {
        data.forEach(row => {
            if (row["NUTS"] !== "") {
                finalVotersNumber[row["NUTS"]] = row["SOUCET_HLASU"];
                totalVoters += parseInt(row["SOUCET_HLASU"]);
            }
        });
        console.log("finalVotersNumber: ", finalVotersNumber);
    }).catch(error => {
        console.error("Error loading the CSV file:", error);
    });

    for (let i = minWave; i <= maxWave; i++) {
        // loading the election results
        d3.csv("./pos_2021_by_okres/" + i + ".csv").then(data => {
            results[i] = data;
        }).catch(error => {
            console.error("Error loading the CSV file:", error);
        });

        // loading the ratio of counted votes
        d3.csv("./pos_2021_by_okres_counted/" + i + ".csv").then(data => {
            let totalCounted = 0;
            countedAt[i] = {};
            data.forEach(row => {
                countedAt[i][row["NUTS"]] = row["counted"];
                totalCounted += parseInt(row["counted"]) * parseInt(finalVotersNumber[row["NUTS"]]);
            });
            countedAtGlobal[i] = totalCounted / totalVoters;
        }).catch(error => {
            console.error("Error loading the CSV file:", error);
        });

        // loading temporary party results
        d3.csv("./pos_2021_by_okres_perc/" + i + ".csv").then(data => {
            partyResults[i] = {};
            data.forEach(row => {
                partyResults[i][row[""]] = row[0];
            });
        }).catch(error => {
            console.error("Error loading the CSV file:", error);
        });
    }

    // Reading the Okres number to Okres name mapping file
    d3.csv("./CIS0109_CS.csv").then(data => {
        //
        data.forEach(row => {
            okresCiselnik[row["chodnota"]] = row["text"];
        });
        console.log("ciselnik: ", okresCiselnik);
    }).catch(error => {
        console.error("Error loading the CSV file:", error);
    });
}


function init() {
        console.log("Total voters: ", totalVoters);

    const svg = d3.select("#map")
        .attr("viewBox", `0 0 ${width} ${height - 50}`);

    // Replace with your actual TopoJSON file path or URL
    const topojsonUrl = "https://raw.githubusercontent.com/karasekadam/map_data/refs/heads/main/okresy2.json";

    // Create a projection and path generator
    const projection = d3.geoMercator()
        .center([15.5, 49.8]) // Center on Czechia
        .scale(6000)
        .translate([width / 2, height / 2]);

    const path = d3.geoPath().projection(projection);

    // Tooltip setup
    const tooltip = d3.select("body")
        .append("div")
        .attr("class", "tooltip")
        .style("position", "absolute")
        .style("background", "white")
        .style("border", "1px solid #ccc")
        .style("padding", "5px")
        .style("border-radius", "5px")
        .style("box-shadow", "0 0 5px rgba(0,0,0,0.3)")
        .style("pointer-events", "none")
        .style("display", "none");

    // Load the TopoJSON file
    d3.json(topojsonUrl).then(topojsonData => {
        // Extract the GeoJSON features from the TopoJSON
        const geojsonFeatures = topojson.feature(topojsonData, topojsonData.objects.data).features;
        // Draw the regions
        svg.selectAll(".region")
            .data(geojsonFeatures)
            .enter()
            .append("path")
            .attr("class", "region")
            .attr("d", path)
            .style("fill", "white")
            .on("mouseover", function(event, d) {
                d3.select(this).classed("hovered", true);
                //Changing the Prague region code to match the one in the results
                if (d.properties.KOD_LAU1 === "CZ0100") {
                    d.properties.KOD_LAU1 = "CZ010";
                }
                let region_results = results[currWave].filter(row => row["NUTS"] === d.properties.KOD_LAU1);
                if (region_results.length === 0) {
                    tooltip.style("display", "block")
                    .html(`<strong>No results yet</strong>`);
                }
                else {
                    tooltip.style("display", "block")
                    .html(`<strong>${okresCiselnik[d.properties.KOD_LAU1]}</strong><br>
                        <ul>
                            <li>number of votes: ${region_results[0]["SOUCET_HLASU"]}</li>
                            <li>counted: ${countedAt[currWave][d.properties.KOD_LAU1]}%</li>
                            <li>ANO: ${(region_results[0]["ANO 2011_votes"] / 
                                    region_results[0]["SOUCET_HLASU"] * 100).toFixed(2)}%</li>
                            <li>SPOLU: ${(region_results[0]["SPOLU – ODS, KDU-ČSL, TOP 09_votes"] / 
                                    region_results[0]["SOUCET_HLASU"] * 100).toFixed(2)}%</li>
                            <li>PIRSTAN: ${(region_results[0]["PIRÁTI a STAROSTOVÉ_votes"] / 
                                    region_results[0]["SOUCET_HLASU"] * 100).toFixed(2)}%</li>
                            <li>SPD: ${(region_results[0]["Svoboda a př. demokracie (SPD)_votes"] / 
                                    region_results[0]["SOUCET_HLASU"] * 100).toFixed(2)}%</li>
                        <ul>`);
                }
            })
            .on("mousemove", function(event) {
                tooltip.style("left", (event.pageX + 10) + "px")
                       .style("top", (event.pageY + 10) + "px");
            })
            .on("mouseout", function() {
                d3.select(this).classed("hovered", false);
                tooltip.style("display", "none");
            });
        calculateCurrTime();
    }).catch(error => {
        console.error("Error loading the TopoJSON file:", error);
    });

    // Set slider
    const slider = document.getElementById("wave");
    const prevButton = document.getElementById("prevWave");
    const nextButton = document.getElementById("nextWave");
    slider.min = minWave;
    slider.max = maxWave;
    slider.value = currWave;

    const sliderValue = document.getElementById("sliderShow");
    const waveTime = document.getElementById("waveTime");
    const globalCounted = document.getElementById("globalCounted");

    function calculateCurrTime() {
        let currTime = new Date((new Date("2021-10-09T15:20:00")).getTime() + currWave*60000*5);
        waveTime.textContent = currTime.toLocaleString();
    }

    function updateGlobalCounted() {
        globalCounted.textContent = (countedAtGlobal[currWave]).toFixed(2) + "%";
    }
    updateGlobalCounted();

    // Update the displayed numbers whenever the slider value changes
    slider.addEventListener("input", () => {
        sliderValue.textContent = slider.value;
        calculateCurrTime();
        updateGlobalCounted();
    });

    // Handle the 'Previous' button
    prevButton.addEventListener("click", function() {
        let sliderValue = parseInt(slider.value);
        if (sliderValue > slider.min) {
            console.log("Curr wave before change: ", currWave);
            let nextValue = sliderValue - 1;
            slider.value = nextValue;
            currWave = nextValue;
            sliderShow.textContent = nextValue;
            calculateCurrTime();
            visualizeCounted();
            updateGlobalCounted();
        }
    });

    // Handle the 'Next' button
    nextButton.addEventListener("click", function() {
        let sliderValue = parseInt(slider.value);
        if (sliderValue < slider.max) {
            console.log("Curr wave before change: ", currWave);
            let nextValue = sliderValue + 1;
            slider.value = nextValue;
            currWave = nextValue;
            sliderShow.textContent = nextValue;
            calculateCurrTime();
            visualizeCounted();
            updateGlobalCounted();
        }
    });

    slider.oninput = function() {
        currWave = this.value;
        visualizeCounted();
    }
}

// Function to visualize the percentage of counted votes in the regions
function visualizeCounted() {
    d3.selectAll("path.region") // Select all regions with class "region"
        .each(function (d, i) {
            const region = d3.select(this);
            let region_code = d.properties.KOD_LAU1;
            if (region_code === "CZ0100") {
                region_code = "CZ010";
            }

            let counted = countedAt[currWave][region_code];
            region.style("fill", colorScale(counted));
      });
}

let slider = document.getElementById("wave");
slider.oninput = function() {
    currWave = this.value;
    visualizeCounted();
}

loadData();
// Wait for the data to load
setTimeout(() => {
    init();
    initGraph();
}, "1000");

function initGraph() {
    // Select SVG and define margins
    const svg = d3.select("#graph"),
        margin = { top: 20, right: 30, bottom: 50, left: 50 },
        width = 500 - margin.left - margin.right,
        height = 400 - margin.top - margin.bottom;

    // Create a group inside SVG for positioning
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    // Extract wave numbers and party names
    const waves = Object.keys(partyResults);
    const parties = Object.keys(partyResults[waves[0]]).filter(party => party.includes("_perc"));
    console.log("parties: ", parties);

    // Create a dataset in array format for D3
    const formattedData = parties.map(party => ({
        party: party,
        values: waves.map(wave => ({ wave: wave, percentage: partyResults[wave][party] }))
    }));

    // Define scales
    const xScale = d3.scaleLinear()
        .domain([waves[0], waves[waves.length - 1]])
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([0, 40])
        .range([height, 0]);

    // Define line generator
    const line = d3.line()
        .x(d => xScale(d.wave))
        .y(d => yScale(d.percentage))
        .curve(d3.curveMonotoneX);

    // Append x-axis
    g.append("g")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(xScale).tickFormat(d3.format("d")));

    // Append y-axis
    g.append("g").call(d3.axisLeft(yScale));

    const partyColors = {
        "Svoboda a př. demokracie (SPD)_percent": "#775c1e",
        "SPOLU – ODS, KDU-ČSL, TOP 09_percent": "#245da5",
        "PIRÁTI a STAROSTOVÉ_percent": "#1c1a1a",
        "ANO 2011_percent": "#33c6bb"
    };

    const partyShortcuts = {
        "ANO 2011_percent": "ANO",
        "Svoboda a př. demokracie (SPD)_percent": "SPD",
        "PIRÁTI a STAROSTOVÉ_percent": "PIRSTAN",
        "SPOLU – ODS, KDU-ČSL, TOP 09_percent": "SPOLU"
};

    // Append lines for each party
    g.selectAll(".line")
        .data(formattedData)
        .enter()
        .append("path")
        .attr("fill", "none")
        .attr("stroke", d => partyColors[d.party])
        .attr("stroke-width", 2)
        .attr("d", d => line(d.values));

    // Add legend
    const legend = g.selectAll(".legend")
        .data(parties)
        .enter().append("g")
        .attr("transform", (d, i) => `translate(${width - 100},${i * 20})`);

    legend.append("rect")
        .attr("width", 10)
        .attr("height", 10)
        .attr("fill", d => partyColors[d]);

    legend.append("text")
        .attr("x", 15)
        .attr("y", 10)
        .text(d => partyShortcuts[d]);

    // add x axis label
    g.append("text")
    .attr("x", width / 2) // Centered
    .attr("y", height + 40) // Below x-axis
    .attr("text-anchor", "middle")
    .style("font-size", "14px")
    .style("fill", "black")
    .text("Result Wave");

    // add y axis label
    g.append("text")
    .attr("transform", "rotate(-90)") // Rotate for Y-axis
    .attr("x", -height / 2) // Centered along Y-axis
    .attr("y", -40) // Shifted left of Y-axis
    .attr("text-anchor", "middle")
    .style("font-size", "14px")
    .style("fill", "black")
    .text("Party %");

}


