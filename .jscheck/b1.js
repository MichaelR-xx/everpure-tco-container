
(function() {
  const $ = id => document.getElementById(id);
  const HELP_CONTENT = {
    workflow: { title: "📖 How to use the Everpure Azure Disk Viz Tool", html: `
      <p>This tool estimates and compares the multi-year cost of running storage on <strong>Azure managed disks</strong> versus <strong>Pure Storage / Everpure</strong>. You upload a workload inventory, map its columns, run a cost analysis, and review the results. Work flows left-to-right across the tabs.</p>
      <h3>1 · Sign in</h3>
      <ul>
        <li>Enter your username and password to access the tool.</li>
      </ul>
      <h3>2 · Customers</h3>
      <ul>
        <li>Select or create a <strong>customer</strong> and a <strong>scenario</strong> (e.g. <code>default</code>). Together they define the folder your data lives under.</li>
      </ul>
      <h3>3 · Data Upload — upload &amp; map your data</h3>
      <ol>
        <li>Drop or browse to a workload inventory <strong>CSV</strong> and upload it.</li>
        <li>Columns are <strong>auto-matched</strong> to known fields; a <strong>data preview</strong> (first 3 rows) shows above the mapping grid.</li>
        <li>Adjust any mapping. <span class="hstep">Disk Type</span> and <span class="hstep">Disk Size</span> are required. Tick <strong>Searchable</strong> on text columns you'll want to filter on later.</li>
        <li>Optionally save/load a <strong>mapping template</strong>. Click <strong>Parse Data</strong> to produce the parsed dataset.</li>
      </ol>
      <h3>4 · Results — run the analysis</h3>
      <ol>
        <li>Pick a parsed dataset from the list.</li>
        <li>Set <strong>Analysis Parameters</strong>: deployment model (<strong>Dedicated</strong> or <strong>Azure Native</strong>), growth, data-reduction ratio, snapshot rate, efficiency, SKU, years, and projection cycle.</li>
        <li><em>Optional</em> — use the <strong>🔎 Search data / Use-case filter</strong> to keep or exclude rows matching terms (e.g. <code>sql</code>, <code>db</code>). <strong>Preview</strong> the split, <strong>Apply to summary</strong> to re-scope the metrics, or <strong>Save filtered dataset</strong> as a new dataset.</li>
        <li>Click <strong>Run Analysis</strong> — this generates a TCO you'll review on the next tab.</li>
      </ol>
      <h3>5 · TCO Review — compare &amp; explore</h3>
      <ul>
        <li>Browse generated TCOs (color-coded by deployment model; filtered runs show a 🔎 badge).</li>
        <li>Apply <strong>commercial adjustments</strong> (min savings, Everpure discount, partner margin, Azure discount, region filter).</li>
        <li>Switch views: <strong>Data</strong> (per-group table), <strong>Graphs</strong> (cost + growth projection, optionally folding in a migration plan), <strong>Migration</strong> (build/save a phased migration schedule), <strong>Advanced</strong> (raw group data), <strong>Downloads</strong> (saved PDFs).</li>
        <li>Set a run as <strong>primary</strong> and have others follow it, or <strong>compare</strong> multiple runs side by side.</li>
      </ul>
      <div class="hnote">Tip: each section has its own <strong>❓ Help</strong> button in the heading for details specific to that page.</div>` },

    login: { title: "❓ Login &amp; Storage Location", html: `
      <p>Enter your username and password, then choose a <strong>storage location</strong> — this is where the tool reads and writes all data. It must be set before you can sign in.</p>
      <h3>Storage options</h3>
      <ul>
        <li><strong>MikeS3</strong> — the default shared S3 bucket, using the server's built-in credentials. Nothing else to fill in.</li>
        <li><strong>Other S3</strong> — your own bucket. Provide the <strong>S3 bucket name</strong>, <strong>AWS access key ID</strong>, and <strong>AWS secret access key</strong>.</li>
        <li><strong>Local Storage</strong> — a drive on the machine running the app. Provide a <strong>drive letter</strong> (e.g. <code>D</code>); data is written under <code>&lt;drive&gt;:\\EverpureTCO</code>.</li>
      </ul>
      <h3>Write test</h3>
      <p>On sign-in the tool writes, reads, and deletes a small test file at the chosen location. If that fails (bad credentials, missing bucket, non-existent or read-only drive) the login is refused with the reason. A brand-new Other-S3/Local backend is automatically seeded with the required engine config files copied from MikeS3.</p>` },

    customers: { title: "❓ Customers", html: `
      <p>A <strong>customer</strong> and <strong>scenario</strong> namespace everything you do — all uploads, parsed data, and TCO runs are stored under <code>&lt;customer&gt;/&lt;scenario&gt;/</code> at your storage location.</p>
      <h3>What to do here</h3>
      <ul>
        <li><strong>Select</strong> an existing customer, or type a name and <strong>Add Customer</strong>.</li>
        <li>Set the <strong>Scenario</strong> name (defaults to <code>default</code>) — use scenarios to keep separate analyses for the same customer (e.g. <code>prod</code>, <code>dr</code>).</li>
        <li><strong>Save Customer &amp; Scenario</strong> to make it active. The active selection drives the Upload, Results, and TCO Review tabs.</li>
        <li>The customer list is stored at <code>TCO-GUI/_config/customer_list.json</code>.</li>
      </ul>` },

    s3upload: { title: "❓ Data Upload — upload &amp; column mapping", html: `
      <p>Upload a workload inventory CSV and tell the tool which columns mean what, then parse it into the dataset the cost engine uses.</p>
      <h3>Steps</h3>
      <ol>
        <li><strong>Upload</strong> — drop a file or click to browse (CSV). For S3 backends the browser uploads directly; for Local Storage it uploads through the server.</li>
        <li><strong>Data preview</strong> — the first 3 rows (chosen to avoid blanks) show above the mapping grid so you can see real values.</li>
        <li><strong>Map columns</strong> — each file column maps to a field. <span class="hstep">Disk Type</span> and <span class="hstep">Disk Size</span> are <strong>required</strong>; <strong>Parse Data</strong> stays disabled until both are set. Choose <code>Don't Use</code> to ignore a column.</li>
        <li><strong>Searchable</strong> — tick columns whose text you'll want to filter on later (host/VM names, tags, resource groups). This powers the Results-page use-case filter.</li>
        <li><strong>Templates</strong> — save the current mapping as a template and load it for similar files. Manual mappings are also learned as aliases for next time.</li>
        <li><strong>Parse Data</strong> — builds the parsed dataset (and its column config) used on the Results tab.</li>
      </ol>` },

    results: { title: "❓ Results — parameters, filtering &amp; running analysis", html: `
      <p>Pick a parsed dataset, review its summary, set the cost assumptions, optionally filter by use case, and run the analysis.</p>
      <h3>Available Runs / summary</h3>
      <p>Select a parsed dataset on the left to see its capacity, cost, group, region, and disk-type breakdowns.</p>
      <h3>Analysis Parameters</h3>
      <ul>
        <li><strong>Deployment model</strong> — <strong>Dedicated</strong> (EC array sizing) or <strong>Azure Native</strong> (capacity + throughput model).</li>
        <li><strong>Growth</strong>, <strong>Data Reduction Ratio</strong>, <strong>Monthly Snapshot Rate</strong>, <strong>Initial Max Size</strong>, <strong>Usage Efficiency</strong>, default <strong>SKU</strong>, <strong>Years</strong>, and <strong>Projection Cycle</strong>. Advanced settings expose pricing term, per-tier limits, and more.</li>
      </ul>
      <h3>🔎 Search data / Use-case filter <span style="font-weight:400;color:var(--muted);">(optional)</span></h3>
      <ul>
        <li>Search the dataset's <strong>searchable</strong> columns for text indicating a use case (presets like Databases, VDI, Backup, or your own terms).</li>
        <li><strong>Include</strong> keeps matching rows; <strong>Exclude</strong> drops them.</li>
        <li><strong>Preview</strong> shows the split and sample rows. <strong>Apply to summary</strong> re-computes the metrics below on just the filtered rows (revert with <em>Show full data</em>). <strong>Save filtered dataset</strong> stores it as a new dataset you can analyze on its own.</li>
      </ul>
      <h3>Run Analysis</h3>
      <p>Generates a TCO from the selected dataset + parameters (and records any filter). Review it on the <strong>TCO Review</strong> tab.</p>` },

    tco: { title: "❓ TCO Review — compare &amp; explore results", html: `
      <p>Browse generated TCOs and drill into cost, growth, and migration. Runs are color-coded by deployment model; runs built from a filtered dataset show a 🔎 badge.</p>
      <h3>Commercial adjustments</h3>
      <p>Tune <strong>minimum savings rate</strong>, <strong>Everpure discount</strong>, <strong>partner margin</strong>, and <strong>Azure discount</strong>, and filter by <strong>region</strong> or include all groups. Every view updates live.</p>
      <h3>Views</h3>
      <ul>
        <li><strong>Data</strong> — per-group table (VMs, volumes, arrays, capacity, licensed cap, cost, blended $/GiB).</li>
        <li><strong>Graphs</strong> — cost comparison + a <strong>growth projection</strong> over time. Optionally <strong>include a migration plan</strong> so Everpure ramps with migration while unmigrated capacity stays on Azure.</li>
        <li><strong>Migration</strong> — set capacity migrated per month (TiB) and per-group precedence/order, then <strong>save named migration plans</strong>.</li>
        <li><strong>Advanced</strong> — the raw per-group sizing data; <strong>Downloads</strong> — saved PDF reports.</li>
      </ul>
      <h3>Primary / follower &amp; compare</h3>
      <p>Mark one run as <strong>primary</strong> so others inherit its included-group set, or select multiple runs and <strong>compare</strong> them side by side. Graph views can be downloaded as PDF.</p>` },
  };

  function openHelp(key) {
    const h = HELP_CONTENT[key] || HELP_CONTENT.workflow;
    $("help-title").innerHTML = h.title;
    $("help-body").innerHTML  = h.html;
    $("help-overlay").style.display = "block";
    $("help-body").parentElement.scrollTop = 0;
  }
  function closeHelp() { $("help-overlay").style.display = "none"; }

  document.querySelectorAll("[data-help]").forEach(b =>
    b.addEventListener("click", () => openHelp(b.dataset.help)));
  $("help-close").addEventListener("click", closeHelp);
  $("help-overlay").addEventListener("click", e => { if (e.target.id === "help-overlay") closeHelp(); });
  document.addEventListener("keydown", e => { if (e.key === "Escape") closeHelp(); });
})();
