import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";
import { DEFAULT_QUERY } from "./query.js";

// Constants
const DEFAULT_QUERY_LIMIT = 100000;
const DEFAULT_PAGE_LENGTH = 25;
const PLOT_HEIGHT = 300;
const PLOT_MIN_WIDTH = 400;
const RESIZE_DEBOUNCE_MS = 150;
const DEFAULT_Y_AXIS_WIDTH = 60;
const CHAR_WIDTH_ESTIMATE = 8;

// Color palette for multi-file plotting
const COLOR_PALETTE = [
    "#4a90e2", // blue
    "#e74c3c", // red
    "#2ecc71", // green
    "#f39c12", // orange
    "#9b59b6", // purple
    "#1abc9c", // teal
    "#e67e22", // dark orange
    "#34495e", // dark gray
    "#16a085", // dark teal
    "#c0392b", // dark red
];

// Global state
let db = null;
let conn = null;
let lastQueryResults = null;
let currentPlots = [];
let resizeHandler = null;
let loadedFiles = []; // Track all loaded files
let plotOptions = {
    xAxisColumn: "filename_index",
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
        ordering: false,
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

    plotOptionsMenu.classList.add("visible");

    // Check if data has filename column (multi-file mode)
    const hasOriginColumn = data[0] && "filename" in data[0];
    const columnNames = Object.keys(data[0]).filter(
        (col) => col !== "filename" && col !== "filename_index"
    );

    // Group data by origin filename if available
    let dataGroups = {};
    let fileColorMap = {};

    if (hasOriginColumn) {
        // Group by filename
        data.forEach((row) => {
            const filename = row.filename;
            if (!dataGroups[filename]) {
                dataGroups[filename] = [];
            }
            dataGroups[filename].push(row);
        });

        // Assign colors to each file
        const filenames = Object.keys(dataGroups);
        filenames.forEach((filename, idx) => {
            fileColorMap[filename] = COLOR_PALETTE[idx % COLOR_PALETTE.length];
        });
    } else {
        // Single file mode
        dataGroups["default"] = data;
        fileColorMap["default"] = plotOptions.lineColor;
    }

    // Populate x-axis dropdown with column names
    xAxisSelect.innerHTML = "";

    // Add filename_index option if in multi-file mode (default)
    if (hasOriginColumn) {
        const fileIndexOption = document.createElement("option");
        fileIndexOption.value = "filename_index";
        fileIndexOption.textContent = "File Index (default)";
        if (plotOptions.xAxisColumn === "filename_index") {
            fileIndexOption.selected = true;
        }
        xAxisSelect.appendChild(fileIndexOption);
    }

    // Add regular index option
    const indexOption = document.createElement("option");
    indexOption.value = "__index__";
    indexOption.textContent = "Global Index";
    if (plotOptions.xAxisColumn === "__index__") {
        indexOption.selected = true;
    }
    xAxisSelect.appendChild(indexOption);

    columnNames.forEach((col) => {
        const option = document.createElement("option");
        option.value = col;
        option.textContent = col;
        if (col === plotOptions.xAxisColumn) {
            option.selected = true;
        }
        xAxisSelect.appendChild(option);
    });

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

        // Create container for this subplot
        const plotDiv = document.createElement("div");
        plotDiv.className = "subplot";
        plotContainer.appendChild(plotDiv);

        // Prepare series data for multi-file mode
        let series = [
            {
                label:
                    plotOptions.xAxisColumn === "__index__"
                        ? "Global Index"
                        : plotOptions.xAxisColumn === "filename_index"
                        ? "File Index"
                        : plotOptions.xAxisColumn,
            },
        ];

        let plotData = [];

        if (hasOriginColumn) {
            // Multi-file mode: create a series for each file
            const filenames = Object.keys(dataGroups);

            // Build combined x-axis data (union of all files' x-axis values)
            let allXValues = [];
            filenames.forEach((filename) => {
                const fileData = dataGroups[filename];
                const xData =
                    plotOptions.xAxisColumn === "__index__"
                        ? fileData.map((_, i) => i)
                        : plotOptions.xAxisColumn === "filename_index"
                        ? fileData.map((row) => {
                              const val = row.filename_index;
                              return val === null || val === undefined
                                  ? null
                                  : Number(val);
                          })
                        : fileData.map((row) => {
                              const val = row[plotOptions.xAxisColumn];
                              return val === null || val === undefined
                                  ? null
                                  : Number(val);
                          });
                allXValues.push(...xData);
            });

            // For index mode, use sequential indices
            if (plotOptions.xAxisColumn === "__index__") {
                plotData.push(Array.from({ length: data.length }, (_, i) => i));
            } else if (plotOptions.xAxisColumn === "filename_index") {
                // For file index, use the actual file index values from data
                plotData.push(data.map((row) => Number(row.filename_index)));
            } else {
                // Sort and deduplicate x values
                allXValues = [...new Set(allXValues)].sort((a, b) => a - b);
                plotData.push(allXValues);
            }

            // Add a series for each file
            filenames.forEach((filename) => {
                const fileData = dataGroups[filename];
                series.push({
                    label: filename,
                    stroke: fileColorMap[filename],
                    width: 2,
                });

                // Extract y data for this file
                const yData = fileData.map((row) => {
                    const val = row[columnName];
                    return val === null || val === undefined
                        ? null
                        : Number(val);
                });

                plotData.push(yData);
            });
        } else {
            // Single-file mode
            const xAxisData =
                plotOptions.xAxisColumn === "__index__"
                    ? data.map((_, i) => i)
                    : data.map((row) => {
                          const val = row[plotOptions.xAxisColumn];
                          return val === null || val === undefined
                              ? null
                              : Number(val);
                      });

            const columnData = data.map((row) => {
                const val = row[columnName];
                return val === null || val === undefined ? null : Number(val);
            });

            plotData = [xAxisData, columnData];

            series.push({
                label: columnName,
                stroke: plotOptions.lineColor,
                width: 2,
            });
        }

        // uPlot options
        const opts = {
            title: columnName,
            width: getPlotWidth(),
            height: PLOT_HEIGHT,
            legend: {
                show: hasOriginColumn, // Show legend in multi-file mode
            },
            cursor: {
                sync: {
                    key: "parkett-plots",
                },
            },
            series: series,
            axes: [
                {
                    grid: { show: true },
                },
                {
                    side: 1,
                    grid: { show: true },
                    size: maxYAxisWidth,
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

// Load and query parquet file(s)
async function loadParquetFile(files, customQuery) {
    try {
        updateProgress(0, "Starting...");

        // Handle both single file and multiple files
        const fileArray = Array.isArray(files) ? files : [files];
        const totalFiles = fileArray.length;

        // Register all files with DuckDB
        for (let i = 0; i < fileArray.length; i++) {
            const file = fileArray[i];
            const progressPercent = Math.floor((i / totalFiles) * 50);
            updateProgress(
                progressPercent,
                `Loading file ${i + 1}/${totalFiles}...`
            );

            console.log(`Loading file: ${file.name}`);
            const arrayBuffer = await file.arrayBuffer();
            const uint8Array = new Uint8Array(arrayBuffer);

            // Register each file with its actual name
            await db.registerFileBuffer(file.name, uint8Array);

            // Track loaded files
            if (!loadedFiles.find((f) => f.name === file.name)) {
                loadedFiles.push({ name: file.name, file: file });
            }
        }

        updateProgress(60, "Executing query...");

        // Use default query if no custom query provided
        const query = customQuery || DEFAULT_QUERY;

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

// Initialize CodeMirror immediately to avoid FOUC
const queryTextarea = document.getElementById("queryInput");
const codeMirror = CodeMirror.fromTextArea(queryTextarea, {
    mode: "text/x-sql",
    theme: "neat",
    lineNumbers: false,
    lineWrapping: true,
    indentWithTabs: false,
    indentUnit: 2,
    tabSize: 2,
});
codeMirror.setSize(null, "auto");

// Initialize on page load
initDuckDB()
    .then(() => {
        const fileInput = document.getElementById("fileInput");
        const fileMapping = document.getElementById("fileMapping");

        // Wire up the file button to trigger file input
        const fileButton = document.getElementById("fileButton");

        fileButton.addEventListener("click", () => {
            fileInput.click();
        });

        // Set default query when file is selected
        fileInput.addEventListener("change", () => {
            const files = Array.from(fileInput.files);
            if (files.length > 0) {
                if (files.length === 1) {
                    fileMapping.textContent = `${files[0].name}`;
                } else {
                    fileMapping.textContent = `${files.length} files selected`;
                }

                // Set the default query from the centralized query template
                codeMirror.setValue(DEFAULT_QUERY);

                fileButton.classList.add("file-loaded");
                document
                    .getElementById("loadButton")
                    .classList.add("file-loaded");
            }
        });

        document
            .getElementById("loadButton")
            .addEventListener("click", async () => {
                const files = Array.from(fileInput.files);

                if (files.length === 0) {
                    alert("Please select a parquet file first");
                    return;
                }

                const query = codeMirror.getValue().trim();
                if (!query) {
                    alert("Please enter a query");
                    return;
                }

                await loadParquetFile(files, query);
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

                // Update plot options
                plotOptions.xAxisColumn = xAxisSelect.value;

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
