import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";

// Constants
const DEFAULT_QUERY_LIMIT = 100000;
const DEFAULT_PAGE_LENGTH = 25;
const PLOT_HEIGHT = 300;
const PLOT_MIN_WIDTH = 400;
const RESIZE_DEBOUNCE_MS = 150;
const DEFAULT_Y_AXIS_WIDTH = 60;
const CHAR_WIDTH_ESTIMATE = 8;

// Global state
let db = null;
let conn = null;
let lastQueryResults = null;
let currentPlots = [];
let resizeHandler = null;
let plotOptions = {
    xAxisColumn: "__index__",
    lineColor: "#4a90e2",
};

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
    const plotButton = document.getElementById("plotButton");

    if (!data || data.length === 0) {
        tableContainer.innerHTML = "<p>No data to display</p>";
        return;
    }

    // Show table, hide plots
    tableContainer.style.display = "block";
    plotContainer.style.display = "none";

    // Update button text
    plotButton.textContent = "Plot Data";

    // Hide plot options menu
    const plotOptionsMenu = document.getElementById("plotOptionsMenu");
    if (plotOptionsMenu) {
        plotOptionsMenu.classList.remove("visible");
    }

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
        <div class="table-scroll-wrapper">
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

    // Initialize DataTable with performance optimizations
    $("#dataTable").DataTable({
        data: data,
        columns: columns,
        pageLength: DEFAULT_PAGE_LENGTH,
        searching: true,
        ordering: true,
        info: true,
        autoWidth: false,
        deferRender: true,
        scroller: false,
        processing: false,
    });
}

// Plot data using uPlot
function plotTable(data) {
    const tableContainer = document.getElementById("tableContainer");
    const plotContainer = document.getElementById("plotContainer");
    const plotButton = document.getElementById("plotButton");

    if (!data || data.length === 0) {
        plotContainer.innerHTML = "<p>No data to plot</p>";
        return;
    }

    // Show plots, hide table
    tableContainer.style.display = "none";
    plotContainer.style.display = "block";
    plotContainer.innerHTML = "";

    // Update button text
    plotButton.textContent = "Show Table";

    // Show and populate plot options menu
    const plotOptionsMenu = document.getElementById("plotOptionsMenu");
    const xAxisSelect = document.getElementById("xAxisSelect");
    const lineColorPicker = document.getElementById("lineColorPicker");

    plotOptionsMenu.classList.add("visible");

    // Populate x-axis dropdown with column names
    xAxisSelect.innerHTML =
        '<option value="__index__">Index (default)</option>';
    const columnNames = Object.keys(data[0]);
    columnNames.forEach((col) => {
        const option = document.createElement("option");
        option.value = col;
        option.textContent = col;
        if (col === plotOptions.xAxisColumn) {
            option.selected = true;
        }
        xAxisSelect.appendChild(option);
    });

    // Set current color
    lineColorPicker.value = plotOptions.lineColor;

    // Get x-axis data based on selected column
    let xAxisData;
    if (plotOptions.xAxisColumn === "__index__") {
        xAxisData = data.map((_, i) => i);
    } else {
        xAxisData = data.map((row) => {
            const val = row[plotOptions.xAxisColumn];
            return val === null || val === undefined ? null : Number(val);
        });
    }

    // Calculate the maximum y-axis width needed across all columns
    let maxYAxisWidth = DEFAULT_Y_AXIS_WIDTH;
    columnNames.forEach((columnName) => {
        const columnData = data.map((row) => {
            const val = row[columnName];
            return val === null || val === undefined ? null : Number(val);
        });

        // Find min and max to estimate tick label lengths
        const validData = columnData.filter((v) => v !== null && !isNaN(v));
        if (validData.length > 0) {
            const min = Math.min(...validData);
            const max = Math.max(...validData);
            const maxLabelLength = Math.max(
                String(Math.floor(min)).length,
                String(Math.floor(max)).length
            );
            const estimatedWidth = maxLabelLength * CHAR_WIDTH_ESTIMATE + 20;
            maxYAxisWidth = Math.max(maxYAxisWidth, estimatedWidth);
        }
    });

    // Remove old resize handler if it exists
    if (resizeHandler) {
        window.removeEventListener("resize", resizeHandler);
        resizeHandler = null;
    }

    // Clear old plots
    currentPlots = [];

    // Calculate initial width after container is visible
    const getPlotWidth = () => {
        const containerWidth = plotContainer.getBoundingClientRect().width;
        return Math.max(containerWidth - 40, PLOT_MIN_WIDTH);
    };

    // Create resize handler
    let resizeTimeout;
    resizeHandler = () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            const newWidth = getPlotWidth();
            currentPlots.forEach((plot) => {
                if (plot && plot.setSize) {
                    plot.setSize({ width: newWidth, height: PLOT_HEIGHT });
                }
            });
        }, RESIZE_DEBOUNCE_MS);
    };

    // Add resize listener
    window.addEventListener("resize", resizeHandler);

    // Create a plot for each column
    columnNames.forEach((columnName) => {
        // Skip the column used for x-axis (unless it's index)
        if (
            columnName === plotOptions.xAxisColumn &&
            plotOptions.xAxisColumn !== "__index__"
        ) {
            return;
        }

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
        const plotData = [xAxisData, columnData];

        // uPlot options
        const opts = {
            title: columnName,
            width: getPlotWidth(),
            height: PLOT_HEIGHT,
            legend: {
                show: false,
            },
            cursor: {
                sync: {
                    key: "parkett-plots",
                },
            },
            series: [
                {
                    label:
                        plotOptions.xAxisColumn === "__index__"
                            ? "Index"
                            : plotOptions.xAxisColumn,
                },
                {
                    label: columnName,
                    stroke: plotOptions.lineColor,
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
                    size: maxYAxisWidth, // Use the fixed maximum width for all plots
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
                            currentPlots.forEach((plot) => {
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
        currentPlots.push(plot);
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
        const query =
            customQuery ||
            `SELECT * FROM 'file.parquet' LIMIT ${DEFAULT_QUERY_LIMIT}`;
        console.log(`Executing query: ${query}`);

        const result = await conn.query(query);

        updateProgress(90, "Loading results...");

        // Convert to array - this is where the time is spent
        const rows = result.toArray().map((row) => row.toJSON());

        // Store results for export
        lastQueryResults = rows;

        updateProgress(100, "Complete!");

        // Render the table immediately (no setTimeout delay)
        hideProgress();
        renderTable(rows);

        // Update button styles to show export and plot are now available
        document.getElementById("exportButton").classList.add("query-executed");
        document.getElementById("plotButton").classList.add("query-executed");
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
        const queryTextarea = document.getElementById("queryInput");
        const fileMapping = document.getElementById("fileMapping");

        // Initialize CodeMirror for SQL syntax highlighting
        const codeMirror = CodeMirror.fromTextArea(queryTextarea, {
            mode: "text/x-sql",
            theme: "neat",
            lineNumbers: false,
            lineWrapping: true,
            indentWithTabs: false,
            indentUnit: 2,
            tabSize: 2,
        });

        // Set initial height
        codeMirror.setSize(null, "auto");

        // Wire up the file button to trigger file input
        const fileButton = document.getElementById("fileButton");

        fileButton.addEventListener("click", () => {
            fileInput.click();
        });

        // Set default query when file is selected
        fileInput.addEventListener("change", () => {
            const file = fileInput.files[0];
            if (file) {
                fileMapping.textContent = `${file.name} → file.parquet`;
                codeMirror.setValue(
                    `SELECT * FROM 'file.parquet' LIMIT ${DEFAULT_QUERY_LIMIT}`
                );

                fileButton.classList.add("file-loaded");
                document
                    .getElementById("loadButton")
                    .classList.add("file-loaded");
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

                const query = codeMirror.getValue().trim();
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

            const plotButton = document.getElementById("plotButton");
            const tableContainer = document.getElementById("tableContainer");
            const plotContainer = document.getElementById("plotContainer");

            // Toggle between table and plot view
            if (plotButton.textContent === "Plot Data") {
                plotTable(lastQueryResults);
            } else {
                renderTable(lastQueryResults);
            }
        });

        // Hamburger menu toggle
        document
            .getElementById("hamburgerButton")
            .addEventListener("click", () => {
                const optionsPanel = document.getElementById("optionsPanel");
                optionsPanel.classList.toggle("open");
            });

        // Apply plot options
        document
            .getElementById("applyOptionsButton")
            .addEventListener("click", () => {
                const xAxisSelect = document.getElementById("xAxisSelect");
                const lineColorPicker =
                    document.getElementById("lineColorPicker");

                // Update plot options
                plotOptions.xAxisColumn = xAxisSelect.value;
                plotOptions.lineColor = lineColorPicker.value;

                // Close the panel
                document
                    .getElementById("optionsPanel")
                    .classList.remove("open");

                // Re-render plots with new options
                if (lastQueryResults) {
                    plotTable(lastQueryResults);
                }
            });
    })
    .catch((error) => {
        console.error("Failed to initialize DuckDB:", error);
    });
