import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";

let db = null;
let conn = null;
let lastQueryResults = null;

// Initialize DuckDB
async function initDuckDB() {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], {
            type: "text/javascript",
        })
    );

    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);

    conn = await db.connect();
    console.log("DuckDB initialized successfully");
}

// Update progress bar
function updateProgress(percent, message) {
    const progressContainer = document.getElementById("progressContainer");
    const progressFill = document.getElementById("progressFill");

    progressContainer.style.display = "block";
    progressFill.style.width = percent + "%";
    progressFill.textContent = message || percent + "%";
}

// Hide progress bar
function hideProgress() {
    document.getElementById("progressContainer").style.display = "none";
}

// Export data to CSV
function exportToCSV(data) {
    if (!data || data.length === 0) {
        alert("No data to export");
        return;
    }

    // Get column headers
    const headers = Object.keys(data[0]);

    // Create CSV content
    let csvContent = headers.join(",") + "\n";

    // Add data rows
    data.forEach((row) => {
        const values = headers.map((header) => {
            let value = row[header];
            // Handle null/undefined
            if (value === null || value === undefined) {
                return "";
            }
            // Escape quotes and wrap in quotes if contains comma, quote, or newline
            value = String(value);
            if (
                value.includes(",") ||
                value.includes('"') ||
                value.includes("\n")
            ) {
                value = '"' + value.replace(/"/g, '""') + '"';
            }
            return value;
        });
        csvContent += values.join(",") + "\n";
    });

    // Create blob and download
    const blob = new Blob([csvContent], {
        type: "text/csv;charset=utf-8;",
    });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);

    link.setAttribute("href", url);
    link.setAttribute("download", "query_results.csv");
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}
function renderTable(data) {
    const tableContainer = document.getElementById("tableContainer");
    const plotContainer = document.getElementById("plotContainer");

    if (!data || data.length === 0) {
        tableContainer.innerHTML = "<p>No data to display</p>";
        return;
    }

    // Show table, hide plots
    tableContainer.style.display = "block";
    plotContainer.style.display = "none";

    // Destroy existing DataTable instance if it exists
    if ($.fn.DataTable.isDataTable("#dataTable")) {
        $("#dataTable").DataTable().clear().destroy();
    }

    // Infer column names from the first row
    const columnNames = Object.keys(data[0]);

    // Build basic table skeleton with header (as in the docs)
    const theadHtml =
        "<thead><tr>" +
        columnNames.map((name) => `<th>${name}</th>`).join("") +
        "</tr></thead>";

    tableContainer.innerHTML = `
        <div style="overflow-x:auto;">
            <table id="dataTable" class="display" style="width:100%">
                ${theadHtml}
            </table>
        </div>
    `;

    // Prepare column definitions for DataTables
    const columns = columnNames.map((name) => ({
        data: name,
        title: name,
    }));

    // Initialize DataTable (very similar to the official examples)
    $("#dataTable").DataTable({
        data: data,
        columns: columns,
        pageLength: 10,
        searching: true,
        ordering: true,
        info: true,
        autoWidth: false,
    });
}

// Plot data using uPlot
function plotTable(data) {
    const tableContainer = document.getElementById("tableContainer");
    const plotContainer = document.getElementById("plotContainer");

    if (!data || data.length === 0) {
        plotContainer.innerHTML = "<p>No data to plot</p>";
        return;
    }

    // Show plots, hide table
    tableContainer.style.display = "none";
    plotContainer.style.display = "block";
    plotContainer.innerHTML = "";

    // Get column names
    const columnNames = Object.keys(data[0]);

    // Create index array (0, 1, 2, ...)
    const indexData = data.map((_, i) => i);

    // Store all plot instances for synchronized zooming
    const plots = [];

    // Create a plot for each column
    columnNames.forEach((columnName) => {
        // Extract column data
        const columnData = data.map((row) => {
            const val = row[columnName];
            // Convert to number if possible, otherwise null
            return val === null || val === undefined ? null : Number(val);
        });

        // Create container for this subplot
        const plotDiv = document.createElement("div");
        plotDiv.className = "subplot";
        plotContainer.appendChild(plotDiv);

        // uPlot data format: [x-axis, y-axis]
        const plotData = [indexData, columnData];

        // uPlot options
        const opts = {
            title: columnName,
            width: plotContainer.offsetWidth - 40,
            height: 200,
            legend: {
                show: false,
            },
            cursor: {
                sync: {
                    key: "parkett-plots",
                },
            },
            series: [
                {},
                {
                    label: columnName,
                    stroke: "#4a90e2",
                    width: 2,
                },
            ],
            axes: [
                {
                    grid: { show: true },
                },
                {
                    side: 1,
                    grid: { show: true },
                },
            ],
            scales: {
                x: {
                    time: false,
                },
            },
            hooks: {
                setScale: [
                    (u, key) => {
                        if (key === "x") {
                            // Sync x-axis across all plots
                            plots.forEach((plot) => {
                                if (plot !== u) {
                                    plot.setScale("x", {
                                        min: u.scales.x.min,
                                        max: u.scales.x.max,
                                    });
                                }
                            });
                        }
                    },
                ],
            },
        };

        // Create plot
        const plot = new uPlot(opts, plotData, plotDiv);
        plots.push(plot);
    });
}

// Load and query parquet file
async function loadParquetFile(file, customQuery) {
    try {
        updateProgress(0, "Starting...");
        console.log(`Loading file: ${file.name}`);

        updateProgress(25, "Reading file...");
        // Read file as ArrayBuffer
        const arrayBuffer = await file.arrayBuffer();
        const uint8Array = new Uint8Array(arrayBuffer);

        updateProgress(50, "Registering in DuckDB...");
        // Register the file in DuckDB as 'file.parquet'
        await db.registerFileBuffer("file.parquet", uint8Array);

        updateProgress(75, "Executing query...");
        // Execute query - always uses 'file.parquet' as the filename
        const query = customQuery || `SELECT * FROM 'file.parquet' LIMIT 1000`;
        console.log(`Executing query: ${query}`);

        const result = await conn.query(query);
        const rows = result.toArray().map((row) => row.toJSON());

        // Store results for export
        lastQueryResults = rows;

        // console.log("Query results:");
        // console.table(rows);
        // console.log("Raw data:", rows);

        updateProgress(100, "Complete!");

        // Render the table
        setTimeout(() => {
            hideProgress();
            renderTable(rows);
        }, 500);
    } catch (error) {
        console.error("Error loading parquet file:", error);
        hideProgress();
        alert("Error loading parquet file: " + error.message);
    }
}

// Initialize on page load
initDuckDB()
    .then(() => {
        const fileInput = document.getElementById("fileInput");
        const queryInput = document.getElementById("queryInput");
        const fileMapping = document.getElementById("fileMapping");

        // Set default query when file is selected
        fileInput.addEventListener("change", () => {
            const file = fileInput.files[0];
            if (file) {
                // Show the mapping
                fileMapping.textContent = `${file.name} → file.parquet`;
                // Query always uses 'file.parquet'
                queryInput.value = `SELECT * FROM 'file.parquet' LIMIT 1000`;
            }
        });

        document
            .getElementById("loadButton")
            .addEventListener("click", async () => {
                const file = fileInput.files[0];

                if (!file) {
                    alert("Please select a parquet file first");
                    return;
                }

                const query = queryInput.value.trim();
                if (!query) {
                    alert("Please enter a query");
                    return;
                }

                await loadParquetFile(file, query);
            });

        document
            .getElementById("exportButton")
            .addEventListener("click", () => {
                if (!lastQueryResults) {
                    alert(
                        "No query results to export. Please run a query first."
                    );
                    return;
                }
                exportToCSV(lastQueryResults);
            });

        document.getElementById("plotButton").addEventListener("click", () => {
            if (!lastQueryResults) {
                alert("No query results to plot. Please run a query first.");
                return;
            }
            plotTable(lastQueryResults);
        });
    })
    .catch((error) => {
        console.error("Failed to initialize DuckDB:", error);
    });
