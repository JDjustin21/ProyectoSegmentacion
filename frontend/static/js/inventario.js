document.addEventListener("DOMContentLoaded", () => {
  "use strict";

  const app = document.getElementById("inventarioApp");
  const cardsContainer = document.getElementById("inventarioCardsContainer");

  if (!app || !cardsContainer) {
    console.error("No se encontró #inventarioApp o #inventarioCardsContainer");
    return;
  }

  const apiDashboardUrl = (app.dataset.apiDashboardUrl || "").trim();
  const apiRefreshBaseUrl = (app.dataset.apiRefreshBaseUrl || "").trim();
  const imageResolverUrl = (app.dataset.imageResolverUrl || "").trim();
  const placeholderUrl = (app.dataset.placeholderUrl || "").trim();
  const cardsPerPage = Number(app.dataset.cardsPerPage || 24);

  if (!apiDashboardUrl) throw new Error("Falta data-api-dashboard-url en #inventarioApp");
  if (!apiRefreshBaseUrl) throw new Error("Falta data-api-refresh-base-url en #inventarioApp");
  if (!imageResolverUrl) throw new Error("Falta data-image-resolver-url en #inventarioApp");

  let referencias = [];
  let currentPage = 1;
  let totalPages = 1;

  const filtros = {
    tipo_portafolio: "",
    linea: "",
    estado: "",
    cuento: "",
    categoria: "",
    referencia_sku: "",
    solo_con_inventario: false,
    solo_sin_inventario: false,
  };

  const $ = (selector) => document.querySelector(selector);

  function norm(value) {
    return (value || "").toString().trim();
  }

  function normLower(value) {
    return norm(value).toLowerCase();
  }

  function formatoNumero(value) {
    const n = Number(value || 0);
    return Number.isFinite(n) ? n.toLocaleString("es-CO") : "0";
  }

  function escapeHtml(value) {
    return norm(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  async function fetchJson(url, options = {}) {
    const res = await fetch(url, {
      headers: {
        "Accept": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    });

    const json = await res.json().catch(() => null);

    if (!res.ok || json?.ok === false) {
      throw new Error(json?.error || `HTTP ${res.status}`);
    }

    return json;
  }

  async function cargarInventario() {
    setLoading(true, "Actualizando inventario...");

    try {
      const payload = obtenerFiltrosPayload();

      const json = await fetchJson(apiDashboardUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      referencias = Array.isArray(json.data?.referencias) ? json.data.referencias : [];
      currentPage = 1;

      pintarKpis(json.data?.kpis || {});
      pintarMeta(json.meta || {});
      actualizarFiltrosOpciones(referencias);
      renderCards();
    } catch (error) {
      console.error(error);
      cardsContainer.innerHTML = `
        <div class="alert alert-danger w-100">
          No fue posible cargar inventario. ${escapeHtml(error.message || "")}
        </div>
      `;
    } finally {
      setLoading(false);
    }
  }

  function obtenerFiltrosPayload() {
    return {
      tipo_portafolio: filtros.tipo_portafolio,
      linea: filtros.linea,
      estado: filtros.estado,
      cuento: filtros.cuento,
      categoria: filtros.categoria,
      referencia_sku: filtros.referencia_sku,
      solo_con_inventario: filtros.solo_con_inventario ? "true" : "",
      solo_sin_inventario: filtros.solo_sin_inventario ? "true" : "",
    };
  }

  function pintarKpis(kpis) {
    setText("#kpiInvReferenciasTotales", formatoNumero(kpis.referencias_totales));
    setText("#kpiInvReferenciasConInventario", formatoNumero(kpis.referencias_con_inventario));
    setText("#kpiInvReferenciasSinInventario", formatoNumero(kpis.referencias_sin_inventario));
    setText("#kpiInvSkuDisponibles", formatoNumero(kpis.sku_disponibles));
    setText("#kpiInvDisponibleTotal", formatoNumero(kpis.disponible_total));
  }

  function pintarMeta(meta) {
    setText(
      "#inventarioMetaTexto",
      `${formatoNumero(meta.total_referencias || 0)} referencias visibles`
    );
  }

  function setText(selector, value) {
    const el = $(selector);
    if (el) el.textContent = value ?? "";
  }

  function renderCards() {
    totalPages = Math.max(1, Math.ceil(referencias.length / cardsPerPage));

    if (currentPage > totalPages) {
      currentPage = 1;
    }

    const start = (currentPage - 1) * cardsPerPage;
    const visibles = referencias.slice(start, start + cardsPerPage);

    if (visibles.length === 0) {
      cardsContainer.innerHTML = `
        <div class="alert alert-light text-center w-100">
          No hay referencias para los filtros seleccionados.
        </div>
      `;
    } else {
      cardsContainer.innerHTML = visibles.map(crearCardInventarioHTML).join("");
    }

    setText("#invPageIndicator", `Página ${currentPage} de ${totalPages}`);

    const prev = $("#invPrevPage");
    const next = $("#invNextPage");

    if (prev) prev.disabled = currentPage <= 1;
    if (next) next.disabled = currentPage >= totalPages;
  }

  function crearCardInventarioHTML(ref) {
    const referencia = norm(ref.referencia_sku);
    const descripcion = norm(ref.descripcion);
    const categoria = norm(ref.categoria);
    const linea = norm(ref.linea);
    const cuento = norm(ref.cuento);
    const estado = norm(ref.estado);
    const tipoPortafolio = norm(ref.tipo_portafolio);
    const color = norm(ref.color);
    const estadoInventario = norm(ref.estado_inventario);

    const existenciaTotal = Number(ref.existencia_total || 0);
    const disponibleTotal = Number(ref.disponible_total || 0);
    const skuDisponibles = Number(ref.sku_disponibles || 0);
    const puntosVenta = Number(ref.puntos_venta_con_inventario || 0);
    const tieneInventario = ref.tiene_inventario === true;

    const badgeInventarioClass = tieneInventario ? "bg-success" : "bg-danger";
    const badgeEstadoClass = estado.toLowerCase() === "activo"
      ? "bg-success"
      : estado.toLowerCase() === "moda"
        ? "bg-warning text-dark"
        : "bg-secondary";

    const imgUrl = `${imageResolverUrl}?ref=${encodeURIComponent(referencia)}`;

    return `
      <div class="product-card inventory-card" data-referencia="${escapeHtml(referencia)}">
        <div class="product-image">
          <img class="ref-img"
            src="${imgUrl}"
            alt="Imagen referencia"
            onerror="
              this.onerror=null;
              this.src='${placeholderUrl}';
              const msg = this.closest('.product-image')?.querySelector('.missing-image-msg');
              if (msg) msg.style.display='block';
            ">
          <div class="missing-image-msg" style="display:none;">
            Falta subir imagen de esta referencia
          </div>
        </div>

        <div class="product-info">
          <div class="product-info-head">
            <div class="product-ref">${escapeHtml(referencia)}</div>
            <div class="product-badges">
              <span class="badge ${badgeInventarioClass}">${escapeHtml(estadoInventario)}</span>
            </div>
          </div>

          <div class="product-name">${escapeHtml(descripcion || "Sin descripción")}</div>

          <div class="product-meta">
            <div><b>Color:</b> ${escapeHtml(color || "—")}</div>
            <div><b>Tipo:</b> ${escapeHtml(tipoPortafolio || "—")}</div>
            <div><b>Estado:</b> <span class="badge ${badgeEstadoClass}">${escapeHtml(estado || "—")}</span></div>
            <div><b>Línea:</b> ${escapeHtml(linea || "—")}</div>
            <div><b>Cuento:</b> ${escapeHtml(cuento || "—")}</div>
            <div><b>Categoría:</b> ${escapeHtml(categoria || "—")}</div>
          </div>

          <div class="inventory-metrics">
            <div>
              <span>Existencia</span>
              <strong>${formatoNumero(existenciaTotal)}</strong>
            </div>
            <div>
              <span>Disponible</span>
              <strong>${formatoNumero(disponibleTotal)}</strong>
            </div>
            <div>
              <span>SKU disp.</span>
              <strong>${formatoNumero(skuDisponibles)}</strong>
            </div>
            <div>
              <span>Tiendas</span>
              <strong>${formatoNumero(puntosVenta)}</strong>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function actualizarFiltrosOpciones(dataset) {
    llenarSelect("#filtroInvTipoPortafolio", dataset.map(x => x.tipo_portafolio), "Todos", filtros.tipo_portafolio);
    llenarSelect("#filtroInvLinea", dataset.map(x => x.linea), "Todas", filtros.linea);
    llenarSelect("#filtroInvEstado", dataset.map(x => x.estado), "Todos", filtros.estado);

    llenarDatalist("#listaInvCuentos", dataset.map(x => x.cuento));
    llenarDatalist("#listaInvCategorias", dataset.map(x => x.categoria));
    llenarDatalist("#listaInvReferencias", dataset.map(x => x.referencia_sku));
  }

  function llenarSelect(selector, values, defaultLabel, selectedValue) {
    const el = $(selector);
    if (!el) return;

    const unique = [...new Set(values.map(norm).filter(Boolean))].sort((a, b) => a.localeCompare(b, "es"));

    el.innerHTML = `<option value="">${defaultLabel}</option>`;

    unique.forEach((value) => {
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = value;
      if (value === selectedValue) opt.selected = true;
      el.appendChild(opt);
    });
  }

  function llenarDatalist(selector, values) {
    const el = $(selector);
    if (!el) return;

    const unique = [...new Set(values.map(norm).filter(Boolean))].sort((a, b) => a.localeCompare(b, "es"));
    el.innerHTML = "";

    unique.forEach((value) => {
      const opt = document.createElement("option");
      opt.value = value;
      el.appendChild(opt);
    });
  }

  function bindEventos() {
    $("#filtroInvTipoPortafolio")?.addEventListener("change", (e) => {
      filtros.tipo_portafolio = norm(e.target.value);
      cargarInventario();
    });

    $("#filtroInvLinea")?.addEventListener("change", (e) => {
      filtros.linea = norm(e.target.value);
      cargarInventario();
    });

    $("#filtroInvEstado")?.addEventListener("change", (e) => {
      filtros.estado = norm(e.target.value);
      cargarInventario();
    });

    $("#filtroInvCuento")?.addEventListener("input", debounce((e) => {
      filtros.cuento = norm(e.target.value);
      cargarInventario();
    }, 350));

    $("#filtroInvCategoria")?.addEventListener("input", debounce((e) => {
      filtros.categoria = norm(e.target.value);
      cargarInventario();
    }, 350));

    $("#filtroInvReferencia")?.addEventListener("input", debounce((e) => {
      filtros.referencia_sku = norm(e.target.value);
      cargarInventario();
    }, 350));

    $("#filtroInvConInventario")?.addEventListener("change", (e) => {
      filtros.solo_con_inventario = e.target.checked === true;

      if (filtros.solo_con_inventario) {
        filtros.solo_sin_inventario = false;
        const sinInv = $("#filtroInvSinInventario");
        if (sinInv) sinInv.checked = false;
      }

      cargarInventario();
    });

    $("#filtroInvSinInventario")?.addEventListener("change", (e) => {
      filtros.solo_sin_inventario = e.target.checked === true;

      if (filtros.solo_sin_inventario) {
        filtros.solo_con_inventario = false;
        const conInv = $("#filtroInvConInventario");
        if (conInv) conInv.checked = false;
      }

      cargarInventario();
    });

    $("#btnActualizarInventario")?.addEventListener("click", () => {
      cargarInventario();
    });

    $("#btnRefrescarBaseInventario")?.addEventListener("click", async () => {
      await refrescarBaseInventario();
    });

    $("#btnLimpiarInventario")?.addEventListener("click", () => {
      limpiarFiltros();
      cargarInventario();
    });

    $("#invPrevPage")?.addEventListener("click", () => {
      if (currentPage > 1) {
        currentPage -= 1;
        renderCards();
      }
    });

    $("#invNextPage")?.addEventListener("click", () => {
      if (currentPage < totalPages) {
        currentPage += 1;
        renderCards();
      }
    });
  }

  async function refrescarBaseInventario() {
    const confirmar = window.confirm(
      "Esto recalculará la base de inventario. Puede tardar algunos segundos. ¿Deseas continuar?"
    );

    if (!confirmar) return;

    setLoading(true, "Refrescando base de inventario...");

    try {
      await fetchJson(apiRefreshBaseUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });

      await cargarInventario();
    } catch (error) {
      console.error(error);
      alert(error.message || "No fue posible refrescar la base de inventario.");
    } finally {
      setLoading(false);
    }
  }

  function limpiarFiltros() {
    filtros.tipo_portafolio = "";
    filtros.linea = "";
    filtros.estado = "";
    filtros.cuento = "";
    filtros.categoria = "";
    filtros.referencia_sku = "";
    filtros.solo_con_inventario = false;
    filtros.solo_sin_inventario = false;

    [
      "#filtroInvTipoPortafolio",
      "#filtroInvLinea",
      "#filtroInvEstado",
      "#filtroInvCuento",
      "#filtroInvCategoria",
      "#filtroInvReferencia",
    ].forEach((selector) => {
      const el = $(selector);
      if (el) el.value = "";
    });

    const conInv = $("#filtroInvConInventario");
    const sinInv = $("#filtroInvSinInventario");

    if (conInv) conInv.checked = false;
    if (sinInv) sinInv.checked = false;

    currentPage = 1;
  }

  function setLoading(isLoading, message = "Actualizando inventario...") {
    const toast = $("#inventarioLoadingToast");
    const btnActualizar = $("#btnActualizarInventario");
    const btnRefresh = $("#btnRefrescarBaseInventario");

    if (btnActualizar) {
      btnActualizar.disabled = isLoading;
      btnActualizar.textContent = isLoading ? "Actualizando..." : "Actualizar vista";
    }

    if (btnRefresh) {
      btnRefresh.disabled = isLoading;
    }

    if (toast) {
      toast.classList.toggle("d-none", !isLoading);
      const text = toast.querySelector(".analiticas-loading-text");
      if (text) text.textContent = message;
    }
  }

  function debounce(fn, delay = 300) {
    let timer = null;

    return (...args) => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => fn(...args), delay);
    };
  }

  bindEventos();
  cargarInventario();
});