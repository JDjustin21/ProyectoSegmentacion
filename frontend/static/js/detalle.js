// frontend/static/js/detalle.js
//
// Responsabilidad de este módulo:
// - Controlar el modal de detalle (no cards, no filtros globales).
// - Cargar tiendas activas por línea (Postgres) y precargar última segmentación.
// - Renderizar controles de tallas (+ / -) por tienda.
// - Guardar segmentación.
// 
// Diseño:
// - Exponemos un único método público: window.SegmentacionDetalle.open(payload)
// - Todo lo demás queda encapsulado aquí.
//

(function () {
  "use strict";

  // =========================
  // 1) Constantes técnicas (contrato backend)
  // =========================
  const API_TIENDAS = "/segmentacion/api/tiendas/activas";
  const API_ULTIMA = "/segmentacion/api/segmentaciones/ultima";
  const API_GUARDAR = "/segmentacion/api/segmentaciones";

  // =========================
  // 2) Helpers puros
  // =========================
  function norm(v) {
    return (v || "").toString().trim();
  }

  function isoNow() {
    return new Date().toISOString();
  }

  function buildQuery(params) {
    const usp = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      const value = norm(v);
      if (value) usp.set(k, value);
    });
    return usp.toString();
  }

  function debounce(fn, delayMs) {
    let t = null;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), delayMs);
    };
  }

  async function fetchJson(url) {
    const res = await fetch(url, { headers: { "Accept": "application/json" } });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status} - ${text}`);
    }
    return res.json();
  }


  function ensureDatalist(inputEl, id) {
    if (!inputEl) return null;

    inputEl.setAttribute("list", id);

    let dl = document.getElementById(id);
    if (!dl) {
      dl = document.createElement("datalist");
      dl.id = id;
      document.body.appendChild(dl);
    }
    return dl;
  }

  function setDatalistOptions(datalistEl, values) {
    if (!datalistEl) return;

    const uniq = Array.from(
      new Set((values || []).map(v => norm(v)).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b));

    datalistEl.innerHTML = uniq.map(v => `<option value="${v}"></option>`).join("");
  }

  function updateModalDatalists(dom, tiendas) {
    const listDep = ensureDatalist(dom.filtroDependencia, "listaDetalleDependencia");
    const listZona = ensureDatalist(dom.filtroZona, "listaDetalleZona");
    const listClima = ensureDatalist(dom.filtroClima, "listaDetalleClima");
    const listTesteo = ensureDatalist(dom.filtroTesteo, "listaDetalleTesteo");
    const listClasif = ensureDatalist(dom.filtroClasificacion, "listaDetalleClasificacion");

    const arr = Array.isArray(tiendas) ? tiendas : [];

    setDatalistOptions(listDep, arr.map(t => t.desc_dependencia || t.dependencia));
    setDatalistOptions(listZona, arr.map(t => t.zona));
    setDatalistOptions(listClima, arr.map(t => t.clima));
    setDatalistOptions(listTesteo, arr.map(t => t.testeo || t.testeo_fnl));
    setDatalistOptions(listClasif, arr.map(t => t.rankin_linea));
  }

  // =========================
  // 3) DOM del modal
  // =========================
  function getDom() {
    return {
      modalEl: document.getElementById("modalDetalleReferencia"),
      tituloEl: document.getElementById("detalleRefTitulo"),
      subtituloEl: document.getElementById("detalleRefSubtitulo"),
      rankingLineaEl: document.getElementById("detalleRankingLinea"),
      footerInfoEl: document.getElementById("detalleFooterInfo"),

      filtroDependencia: document.getElementById("detalleFiltroDependencia"),
      filtroZona: document.getElementById("detalleFiltroZona"),
      filtroClima: document.getElementById("detalleFiltroClima"),
      filtroTesteo: document.getElementById("detalleFiltroTesteo"),
      filtroClasificacion: document.getElementById("detalleFiltroClasificacion"),

      btnLimpiar: document.getElementById("btnDetalleLimpiar"),
      btnPresetSku: document.getElementById("btnDetallePresetSku"),
      btnGuardar: document.getElementById("btnDetalleGuardar"),

      loadingEl: document.getElementById("detalleLoading"),
      errorEl: document.getElementById("detalleError"),
      tiendasContainer: document.getElementById("detalleTiendasContainer"),
    };
  }

  // =========================
  // 4) Estado interno del modal
  // =========================
  const state = {
    sessionStartIso: null,
    fetchSeq: 0, // evita respuestas cruzadas

    ref: {
      referenciaSku: "",
      descripcion: "",
      categoria: "",
      estado: "",
      tipoPortafolio: "",
      lineaRaw: "",
      lineaTexto: "",
      color: "",
      cuento: "",
      codigoBarras: "",
      tipoInventario: "",
      tallasFinal: [],
      tallasConteo: null,
    },

    tiendas: [],
    cantidades: {},
  };

  // =========================
  // 5) UI helpers: loading / error
  // =========================
  function setLoading(dom, isLoading) {
    if (!dom.loadingEl) return;
    dom.loadingEl.style.display = isLoading ? "block" : "none";
  }

  function setError(dom, message) {
    if (!dom.errorEl) return;
    dom.errorEl.style.display = message ? "block" : "none";
    dom.errorEl.textContent = message || "";
  }

  // =========================
  // 6) Render: tiendas + tallas
  // =========================
  function renderTiendas(dom) {
    const tallas = Array.isArray(state.ref.tallasFinal) ? state.ref.tallasFinal : [];
    dom.tiendasContainer.innerHTML = "";

    const tiendas = Array.isArray(state.tiendas) ? state.tiendas : [];
    if (tiendas.length === 0) {
      dom.tiendasContainer.innerHTML = `
        <div class="alert alert-warning mb-0">
          No se encontraron tiendas activas para esta línea (o con los filtros actuales).
        </div>
      `;
      return;
    }

    const html = tiendas.map(t => {
      const llave = norm(t.llave_naval);
      const nombreTienda = norm(t.desc_dependencia) || norm(t.dependencia);
      const ciudad = norm(t.ciudad);
      const clima = norm(t.clima);
      const zona = norm(t.zona);

      const rankinLinea = norm(t.rankin_linea || "");
      const testeoFnl = norm(t.testeo || t.testeo_fnl || "");

      const ventaProm = norm(t.venta_promedio || "—");
      const cpd = norm(t.cpd || "—");
      const rot = norm(t.rotacion || "—");

      if (!state.cantidades[llave]) state.cantidades[llave] = {};

      const tallasHtml = tallas.map(talla => {
        const keyTalla = norm(talla);
        const qty = Number(state.cantidades[llave][keyTalla] || 0);

        return `
          <div class="detalle-talla-col">
            <div class="detalle-talla-label">${keyTalla}</div>

            <button type="button"
              class="btn btn-sm btn-outline-light btn-qty"
              data-action="plus"
              data-llave="${llave}"
              data-talla="${keyTalla}">+</button>

            <input class="form-control form-control-sm detalle-qty-box"
              value="${qty}"
              readonly
              data-llave="${llave}"
              data-talla="${keyTalla}" />

            <button type="button"
              class="btn btn-sm btn-outline-light btn-qty"
              data-action="minus"
              data-llave="${llave}"
              data-talla="${keyTalla}">-</button>
          </div>
        `;
      }).join("");

      return `
        <div class="detalle-row">
          <div class="detalle-row-left">
            <div class="dep">${nombreTienda}</div>
            <div class="meta">${ciudad} / ${zona} / ${clima}</div>
            <div class="small">Rankin_linea: ${rankinLinea || "—"} • Testeo: ${testeoFnl || "—"}</div>
            <div class="small">Llave: ${llave}</div>
          </div>

          <div class="detalle-row-mid">
            <div>Venta Promedio: ${ventaProm}</div>
            <div>CPD: ${cpd}</div>
            <div>% Rotación: ${rot}</div>
          </div>

          <div class="detalle-tallas">
            ${tallasHtml}
          </div>
        </div>
      `;
    }).join("");

    dom.tiendasContainer.innerHTML = html;
  }

  // =========================
  // 7) Acciones: +/- (delegación)
  // =========================
  function attachDelegatedQtyHandler(dom) {
    dom.tiendasContainer.addEventListener("click", (ev) => {
      const btn = ev.target.closest(".btn-qty");
      if (!btn) return;

      const action = norm(btn.dataset.action);
      const llave = norm(btn.dataset.llave);
      const talla = norm(btn.dataset.talla);

      if (!llave || !talla) return;

      const current = Number(state.cantidades?.[llave]?.[talla] || 0);

      let next = current;
      if (action === "plus") next = current + 1;
      if (action === "minus") next = Math.max(0, current - 1);

      if (!state.cantidades[llave]) state.cantidades[llave] = {};
      state.cantidades[llave][talla] = next;

      const input = dom.tiendasContainer.querySelector(
        `input[data-llave="${llave}"][data-talla="${talla}"]`
      );
      if (input) input.value = String(next);
    });
  }

  // =========================
  // 8) Cargar tiendas (desde backend)
  // =========================
  function extractListDeep(value) {
    if (Array.isArray(value)) return value;

    if (value && typeof value === "object") {
      const candidates = ["data", "rows", "tiendas", "items", "result"];

      for (const key of candidates) {
        if (Array.isArray(value[key])) return value[key];
      }

      for (const key of candidates) {
        if (value[key] && typeof value[key] === "object") {
          const deeper = extractListDeep(value[key]);
          if (Array.isArray(deeper)) return deeper;
        }
      }
    }
    return [];
  }

  async function cargarTiendas(dom) {
    const lineaParaConsulta = norm(state.ref.lineaTexto) || norm(state.ref.lineaRaw);

    const q = buildQuery({
      linea: lineaParaConsulta,
      dependencia: dom.filtroDependencia?.value,
      zona: dom.filtroZona?.value,
      clima: dom.filtroClima?.value,
      testeo: dom.filtroTesteo?.value,
      clasificacion: dom.filtroClasificacion?.value,
    });

    const url = (`${API_TIENDAS}?${q}`).replace(/[\r\n]/g, "");
    console.log("[DETALLE] GET tiendas:", JSON.stringify(url));

    const json = await fetchJson(url);
    if (!json.ok) throw new Error(json.error || "Error consultando tiendas activas");

    state.tiendas = extractListDeep(json.data);
    updateModalDatalists(dom, state.tiendas);
  }

  // =========================
  // 9) Cargar última segmentación
  // =========================
  async function cargarUltimaSegmentacion() {
    const q = buildQuery({ referenciaSku: state.ref.referenciaSku });
    const url = `${API_ULTIMA}?${q}`;
    const json = await fetchJson(url);

    if (!json.ok) return;

    const data = json.data || {};
    const seg = data.segmentacion || {};
    const detalle = Array.isArray(seg.detalle) ? seg.detalle : (Array.isArray(data.detalle) ? data.detalle : []);

    detalle.forEach(d => {
      const llave = norm(d.llave_naval);
      const talla = norm(d.talla);
      const cantidad = Number(d.cantidad || 0);

      if (!llave || !talla) return;
      if (!state.cantidades[llave]) state.cantidades[llave] = {};
      state.cantidades[llave][talla] = cantidad;
    });
  }

  // =========================
  // 10) Preset y limpiar
  // =========================
  function limpiarCantidades() {
    state.cantidades = {};
  }

  function limpiarFiltros(dom) {
    if (dom.filtroDependencia) dom.filtroDependencia.value = "";
    if (dom.filtroZona) dom.filtroZona.value = "";
    if (dom.filtroClima) dom.filtroClima.value = "";
    if (dom.filtroTesteo) dom.filtroTesteo.value = "";
    if (dom.filtroClasificacion) dom.filtroClasificacion.value = "";
  }

  function aplicarPresetDesdeSku() {
    const preset = state.ref.tallasConteo;
    if (!preset || typeof preset !== "object") return;

    state.tiendas.forEach(t => {
      const llave = norm(t.llave_naval);
      if (!llave) return;

      if (!state.cantidades[llave]) state.cantidades[llave] = {};

      state.ref.tallasFinal.forEach(talla => {
        const keyTalla = norm(talla);
        const qty = Number(preset[keyTalla] || 0);
        state.cantidades[llave][keyTalla] = qty;
      });
    });
  }

  // =========================
  // 11) Guardar segmentación
  // =========================
  function buildPayloadGuardar() {
    const payload = {
      referenciaSku: state.ref.referenciaSku,
      descripcion: state.ref.descripcion,
      categoria: state.ref.categoria,
      linea: state.ref.lineaRaw,               // importante: backend guarda linea como viene
      tipo_portafolio: state.ref.tipoPortafolio,
      estado_sku: state.ref.estado,
      cuento: state.ref.cuento,
      codigo_barras: state.ref.codigoBarras,
      tipo_inventario: state.ref.tipoInventario,
    };

    const detalle = [];
    Object.entries(state.cantidades).forEach(([llave_naval, byTalla]) => {
      Object.entries(byTalla || {}).forEach(([talla, cantidad]) => {
        const qty = Number(cantidad || 0);
        if (qty > 0) detalle.push({ llave_naval, talla, cantidad: qty });
      });
    });

    payload.detalle = detalle;
    return payload;
  }

  async function guardarSegmentacion(dom) {
    const payload = buildPayloadGuardar();

    if (!payload.referenciaSku) throw new Error("Falta referenciaSku para guardar.");
    if (!payload.linea) throw new Error("Falta línea para guardar.");
    if (!Array.isArray(payload.detalle) || payload.detalle.length === 0) {
      throw new Error("No hay cantidades para guardar (todas están en 0).");
    }

    const res = await fetch(API_GUARDAR, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Error guardando: HTTP ${res.status} - ${text}`);
    }

    const json = await res.json();
    if (json.ok === false) throw new Error(json.error || "Error guardando segmentación");
    return json;
  }

  // =========================
  // 12) Open modal (API pública)
  // =========================
  async function open(payload) {
    const dom = getDom();
    if (!dom.modalEl) throw new Error("No existe el modal #modalDetalleReferencia en el HTML.");

    state.sessionStartIso = isoNow();

    state.ref.referenciaSku = norm(payload.referenciaSku);
    state.ref.descripcion = norm(payload.descripcion);
    state.ref.categoria = norm(payload.categoria);
    state.ref.estado = norm(payload.estado);
    state.ref.tipoPortafolio = norm(payload.tipoPortafolio);
    state.ref.lineaRaw = norm(payload.lineaRaw);
    state.ref.lineaTexto = norm(payload.lineaTexto);
    state.ref.color = norm(payload.color);
    state.ref.cuento = norm(payload.cuento);
    state.ref.codigoBarras = norm(payload.codigoBarras);
    state.ref.tipoInventario = norm(payload.tipoInventario);

    state.ref.tallasFinal = Array.isArray(payload.tallasFinal)
      ? payload.tallasFinal.map(norm).filter(Boolean)
      : [];
    state.ref.tallasConteo = payload.tallasConteo || null;

    // Reset modal
    state.tiendas = [];
    state.cantidades = {};

    // Header
    dom.tituloEl.textContent = state.ref.referenciaSku || "Detalle de referencia";
    dom.subtituloEl.textContent = `${state.ref.descripcion} • ${state.ref.categoria} • ${state.ref.lineaTexto || state.ref.lineaRaw || "Sin línea"}`;
    dom.footerInfoEl.textContent = `Tallas: ${state.ref.tallasFinal.join(", ")}`;
    if (dom.rankingLineaEl) dom.rankingLineaEl.textContent = "—";

    // Mostrar modal
    const modal = bootstrap.Modal.getOrCreateInstance(dom.modalEl);
    modal.show();

    setError(dom, "");
    setLoading(dom, true);

    try {
      // Importante: al abrir, dejamos filtros en blanco (coherencia)
      limpiarFiltros(dom);

      // 1) Cargar tiendas (sin filtros)
      await cargarTiendas(dom);

      // 2) Cargar última segmentación
      await cargarUltimaSegmentacion();

      // 3) Render
      renderTiendas(dom);

    } catch (err) {
      setError(dom, err?.message || "Error cargando detalle.");
      state.tiendas = [];
      dom.tiendasContainer.innerHTML = "";
    } finally {
      setLoading(dom, false);
    }
  }

  // =========================
  // 13) Listeners del modal (una sola vez)
  // =========================
  function initModalEventsOnce() {
    const dom = getDom();
    if (!dom.modalEl) return;

    attachDelegatedQtyHandler(dom);

    const refetch = async () => {
      const mySeq = ++state.fetchSeq;

      setError(dom, "");
      setLoading(dom, true);

      try {
        await cargarTiendas(dom);

        // Si ya hubo otra búsqueda después, ignoramos esta respuesta
        if (mySeq !== state.fetchSeq) return;

        renderTiendas(dom);

      } catch (err) {
        setError(dom, err?.message || "Error filtrando tiendas.");
      } finally {
        if (mySeq === state.fetchSeq) setLoading(dom, false);
      }
    };

    const refetchDebounced = debounce(refetch, 250);

    // Escuchamos TODOS los filtros del modal
    dom.filtroDependencia?.addEventListener("input", refetchDebounced);
    dom.filtroZona?.addEventListener("input", refetchDebounced);
    dom.filtroClima?.addEventListener("input", refetchDebounced);
    dom.filtroTesteo?.addEventListener("input", refetchDebounced);
    dom.filtroClasificacion?.addEventListener("input", refetchDebounced);

    // Limpiar = limpia filtros + cantidades + recarga
    dom.btnLimpiar?.addEventListener("click", async () => {
      limpiarFiltros(dom);
      limpiarCantidades();
      await refetch();
    });

    dom.btnPresetSku?.addEventListener("click", () => {
      aplicarPresetDesdeSku();
      renderTiendas(dom);
    });

    dom.btnGuardar?.addEventListener("click", async () => {
      setError(dom, "");
      setLoading(dom, true);

      try {
        await guardarSegmentacion(dom);
        dom.footerInfoEl.textContent = `Guardado exitoso • ${new Date().toLocaleString()}`;
      } catch (err) {
        setError(dom, err?.message || "Error guardando.");
      } finally {
        setLoading(dom, false);
      }
    });
  }

  initModalEventsOnce();
  window.SegmentacionDetalle = { open };

})();
