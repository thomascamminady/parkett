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

// Render table using DataTables
function renderTable(data) {
    if (data.length === 0) {
        document.getElementById("tableContainer").innerHTML =
            "<p>No data to display</p>";
        return;
    }

    // Get column names from first row
    const columns = Object.keys(data[0]).map((key) => ({
        title: key,
        data: key,
    }));

    // Clear previous table if exists
    const tableContainer = document.getElementById("tableContainer");
    tableContainer.innerHTML =
        '<div style="overflow-x: auto;"><table id="dataTable" class="display" style="width:100%"></table></div>';

    // Initialize DataTable
    $("#dataTable").DataTable({
        data: data,
        columns: columns,
        pageLength: 10,
        destroy: true,
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
        // Register the file in DuckDB
        await db.registerFileBuffer(file.name, uint8Array);

        updateProgress(75, "Executing query...");
        // Execute query - use custom query or default
        const query = customQuery || `SELECT * FROM '${file.name}' LIMIT 10`;
        console.log(`Executing query: ${query}`);

        const result = await conn.query(query);
        const rows = result.toArray().map((row) => row.toJSON());

        // Store results for export
        lastQueryResults = rows;

        console.log("Query results:");
        console.table(rows);
        console.log("Raw data:", rows);

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

        // Set default query when file is selected
        fileInput.addEventListener("change", () => {
            const file = fileInput.files[0];
            if (file) {
                const newQuery = `SELECT * FROM '${file.name}' LIMIT 10`;
                queryInput.value = newQuery;
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
    })
    .catch((error) => {
        console.error("Failed to initialize DuckDB:", error);
    });
