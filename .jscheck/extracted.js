const THEME_OPTIONS = [
    ["everpure", "Everpure"], ["wipro", "Wipro"], ["accenture", "Accenture"],
    ["ahead", "AHEAD"], ["kyndryl", "Kyndryl"],
  ];
function renderCustomerList(customers) {
    listContainer.innerHTML = "";
    if (!customers.length) {
      listContainer.innerHTML = `<div style="color:var(--muted);font-size:.82rem;">No customers yet — create one on the right.</div>`;
      return;
    }
    customers.forEach(name => {
      const row = document.createElement("div");
      row.style.cssText = "display:flex;align-items:center;gap:8px;";
      const isActive = name === activeCustomer;
      const curT = customerThemes[name] || "everpure";
      const curTLabel = escHtml((THEME_OPTIONS.find(o => o[0] === curT) || [, curT])[1]);
      // A badge always shows the customer's current theme. From the Everpure
      // master view a dropdown is added next to it to reassign; under a brand
      // theme only the badge shows (reassignment is Everpure-only).
      const onEverpure = curTheme() === "everpure";
      const themeBadge = `<span class="badge" title="Theme this customer is visible under" style="margin-left:auto;">${curTLabel}</span>`;
      const themeCtl = onEverpure
        ? `${themeBadge}<select class="customer-theme-select" title="Reassign this customer to another theme"
            style="padding:4px 8px;background:var(--surface2);border:1px solid var(--border);border-radius:7px;color:var(--text);font-size:.72rem;cursor:pointer;">${
              THEME_OPTIONS.map(([v, lab]) => `<option value="${v}"${v === curT ? " selected" : ""}>${lab}</option>`).join("")
            }</select>`
        : themeBadge;
      row.innerHTML = `
        <button class="btn-sm customer-select-btn" data-name="${escHtml(name)}"
          style="${isActive ? "background:var(--accent);border-color:var(--accent);color:#fff;" : ""}">
          ${isActive ? "✔ " : ""}${escHtml(name)}
        </button>
        ${isActive ? `<span class="badge green">active</span>` : ""}
        ${themeCtl}
        <button class="btn-sm customer-delete-btn" title="Remove customer and all its data"
          style="padding:4px 9px;color:var(--danger);border-color:var(--danger);">✕</button>
      `;
      row.querySelector(".customer-select-btn").addEventListener("click", () => selectCustomer(name));
      row.querySelector(".customer-delete-btn").addEventListener("click", () => deleteCustomer(name));
      const themeSel = row.querySelector(".customer-theme-select");
      if (themeSel) themeSel.addEventListener("change", (e) => reassignCustomerTheme(name, e.target.value));
      listContainer.appendChild(row);
    });
  }

