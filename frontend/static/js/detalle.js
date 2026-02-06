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
  function updateFooterStats(dom) {
    const tiendas = Array.isArray(state.tiendas) ? state.tiendas : [];
    const tallas = Array.isArray(state.ref.tallasFinal) ? state.ref.tallasFinal : [];

    // Tiendas activas: toggles en ON dentro de la lista actual
    let activas = 0;

    // “Unidades asignadas” según tu definición nueva:
    // = cuántas tiendas están “segmentadas” en ese momento
    // = activas y con al menos una talla > 0
    let unidadesAsignadas = 0;

    tiendas.forEach(t => {
      const llave = norm(t.llave_naval);
      if (!llave) return;

      if (isStoreActive(llave)) {
        activas += 1;

        const byTalla = state.cantidades?.[llave] || {};
        tallas.forEach(talla => {
          const k = norm(talla);
          unidadesAsignadas += Number(byTalla?.[k] || 0);
        });
      }
    });

    if (dom.tiendasActivasNumEl) dom.tiendasActivasNumEl.textContent = String(activas);
    if (dom.tiendasSegmentadasNumEl) dom.tiendasSegmentadasNumEl.textContent = String(unidadesAsignadas);
  }

  function perfilClass(rankinLinea) {
    const r = norm(rankinLinea).toUpperCase();

    // AA y A = verde
    if (r === "AA" || r === "A") return "perfil-green";

    // B = amarillo
    if (r === "B") return "perfil-yellow";

    // C o NA (o cualquier cosa rara) = rojo
    if (r === "C" || r === "NA" || r === "N/A") return "perfil-red";

    // Por defecto (si viene vacío) lo tratamos como rojo también
    return "perfil-red";
  }


  // =========================
  // 3) DOM del modal
  // =========================
  function getDom() {
    return {
      modalEl: document.getElementById("modalDetalleReferencia"),
      tituloEl: document.getElementById("detalleRefTitulo"),
      subtituloEl: document.getElementById("detalleRefSubtitulo"),
      imgEl: document.getElementById("detalleImg"),
      imgMsgEl: document.getElementById("detalleImgMsg"),
      rankingLineaEl: document.getElementById("detalleRankingLinea"),
      footerInfoEl: document.getElementById("detalleFooterInfo"),

      tiendasActivasNumEl: document.getElementById("detalleTiendasActivasNum"),
      tiendasSegmentadasNumEl: document.getElementById("detalleTiendasSegmentadasNum"),
      btnCancelar: document.getElementById("btnDetalleCancelar"),

      filtroDependencia: document.getElementById("detalleFiltroDependencia"),
      filtroZona: document.getElementById("detalleFiltroZona"),
      filtroClima: document.getElementById("detalleFiltroClima"),
      filtroTesteo: document.getElementById("detalleFiltroTesteo"),
      filtroClasificacion: document.getElementById("detalleFiltroClasificacion"),

      btnLimpiar: document.getElementById("btnDetalleLimpiar"),
      btnPresetSku: document.getElementById("btnDetallePresetSku"),
      btnActivarTodas: document.getElementById("btnDetalleActivarTodas"),
      btnDesactivarTodas: document.getElementById("btnDetalleDesactivarTodas"),
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
      precioUnitario: 0,
      tallasFinal: [],
      tallasConteo: null,
    },

    tiendas: [],
    cantidades: {},
    activoPorTienda: {},
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

  function badgeClassForEstado(estado) {
  const e = norm(estado).toLowerCase();
  if (e === "activo") return "bg-success";
  if (e === "inactivo") return "bg-danger";
  if (e === "moda") return "bg-warning text-dark";
  return "bg-secondary";
}
function toNumberOrZero(v) {
  const n = Number((v ?? "").toString().replace(",", ".").trim());
  return Number.isFinite(n) ? n : 0;
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

    const headerHtml = `
      <div class="detalle-row-5 detalle-row-head">
        <div class="detalle-col">Tienda</div>
        <div class="detalle-col">Perfil</div>
        <div class="detalle-col">Activo</div>
        <div class="detalle-col">Tallas</div>
        <div class="detalle-col">Rotación Hist.</div>
      </div>
    `;

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

      const isActive = isStoreActive(llave);

      const tallasHtml = tallas.map(talla => {
        const keyTalla = norm(talla);
        const qty = Number(state.cantidades[llave][keyTalla] || 0);

        const disabledAttr = isActive ? "" : "disabled";
        const disabledClass = isActive ? "" : "is-disabled";

        return `
          <div class="detalle-talla-col ${disabledClass}">
            <div class="detalle-talla-label">${keyTalla}</div>

            <input
              type="number"
              min="0"
              step="1"
              class="form-control form-control-sm detalle-qty-box"
              value="${qty}"
              data-llave="${llave}"
              data-talla="${keyTalla}"
              ${isActive ? "" : "disabled"}
            />
          </div>
        `;

      }).join("");

      return `
        <div class="detalle-row-5">
          <!-- 1) Tienda -->
          <div class="detalle-col">
            <div class="dep">${nombreTienda}</div>
            <div class="meta">${ciudad} / ${zona} / ${clima}</div>
            <div class="small">Testeo: ${testeoFnl || "—"}</div>
          </div>

          <!-- 2) Rankin -->
          <div class="detalle-col">
            <span class="perfil-badge ${perfilClass(rankinLinea)}">
              ${(rankinLinea || "NA").toUpperCase()}
            </span>
          </div>

          <!-- 3) Activo -->
          <div class="detalle-col detalle-col-activo">
            <div class="active-toggle ${isActive ? "active" : ""}"
                data-llave="${llave}"
                title="${isActive ? "Activo" : "Inactivo"}"></div>
          </div>

          <!-- 4) Tallas -->
          <div class="detalle-col tallas">
            <div class="detalle-tallas">
              ${tallasHtml}
            </div>
          </div>

          <!-- 5) Rotación (placeholder por ahora) -->
          <div class="detalle-col">
            <div class="small">Índice: ${rot}</div>
            <div class="small">Venta Promedio: ${ventaProm}</div>
            <div class="small">CPD: ${cpd}</div>
          </div>
        </div>
      `;
    }).join("");

    dom.tiendasContainer.innerHTML = headerHtml + html;
    updateFooterStats(dom);
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
      if (cantidad > 0) state.activoPorTienda[llave] = true;
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

      let anyPositive = false;

      state.ref.tallasFinal.forEach(talla => {
        const keyTalla = norm(talla);
        const qty = Number(preset[keyTalla] || 0);
        state.cantidades[llave][keyTalla] = qty;
        if (qty > 0) anyPositive = true;
      });

      // si el preset asigna algo, activamos la tienda
      if (anyPositive) state.activoPorTienda[llave] = true;
    });
  }

  function isStoreActive(llave) {
    return state.activoPorTienda[llave] === true;
  }

  function setStoreActive(llave, active, { clearQty } = { clearQty: false }) {
    if (!llave) return;

    state.activoPorTienda[llave] = !!active;

    if (!active && clearQty) {
      if (!state.cantidades[llave]) state.cantidades[llave] = {};
      state.ref.tallasFinal.forEach(t => {
        const talla = norm(t);
        if (!talla) return;
        state.cantidades[llave][talla] = 0;
      });
    }
  }

  function setAllStoresActive(active, { clearQty } = { clearQty: false }) {
    const tiendas = Array.isArray(state.tiendas) ? state.tiendas : [];
    tiendas.forEach(t => {
      const llave = norm(t.llave_naval);
      if (!llave) return;
      setStoreActive(llave, active, { clearQty });
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
      linea: state.ref.lineaRaw,               
      tipo_portafolio: state.ref.tipoPortafolio,
      precio_unitario: toNumberOrZero(state.ref.precioUnitario),
      estado_sku: state.ref.estado,
      cuento: state.ref.cuento,
      codigo_barras: state.ref.codigoBarras,
      tipo_inventario: state.ref.tipoInventario,
    };

    const detalle = [];
    Object.entries(state.cantidades).forEach(([llave_naval, byTalla]) => {
      if (!isStoreActive(llave_naval)) return;

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
    if (!Array.isArray(payload.detalle)) payload.detalle = [];

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
    state.ref.precioUnitario = toNumberOrZero(
      payload.precioUnitario ?? payload.precio_unitario ?? payload.PrecioUnitario
    );

    state.ref.tallasFinal = Array.isArray(payload.tallasFinal)
      ? payload.tallasFinal.map(norm).filter(Boolean)
      : [];
    state.ref.tallasConteo = payload.tallasConteo || null;

    // Reset modal
    state.tiendas = [];
    state.cantidades = {};
    state.activoPorTienda = {};


    // Header
    dom.tituloEl.textContent = state.ref.referenciaSku || "Detalle de referencia";
    dom.subtituloEl.textContent = `${state.ref.descripcion} • ${state.ref.categoria} • ${state.ref.lineaTexto || state.ref.lineaRaw || "Sin línea"}`;

    // =========================
    // Imagen del SKU en el modal (igual que en las cards)
    // =========================
    const cardsContainer = document.getElementById("cards-container");
    const placeholderUrl = (cardsContainer?.dataset.placeholderUrl || "https://via.placeholder.com/120x120?text=IMG").trim();

    const refForImg = state.ref.referenciaSku;
    const imgUrl = `/segmentacion/api/imagenes/referencia?ref=${encodeURIComponent(refForImg)}`;

    if (dom.imgMsgEl) {
      dom.imgMsgEl.style.display = "none";
      dom.imgMsgEl.textContent = "";
    }

    if (dom.imgEl) {
      dom.imgEl.onload = () => {
        if (dom.imgMsgEl) {
          dom.imgMsgEl.style.display = "none";
          dom.imgMsgEl.textContent = "";
        }
      };

      dom.imgEl.onerror = () => {
        dom.imgEl.src = placeholderUrl;

        if (dom.imgMsgEl) {
          dom.imgMsgEl.textContent = `Falta subir imagen para: ${refForImg}`;
          dom.imgMsgEl.style.display = "block";
        }
      };

      dom.imgEl.src = imgUrl;
    }

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

    dom.tiendasContainer.addEventListener("click", (ev) => {
      const toggle = ev.target.closest(".active-toggle");
      if (!toggle) return;

      const llave = norm(toggle.dataset.llave);
      if (!llave) return;

      const willBeActive = !isStoreActive(llave);

      // Si se apaga, limpiamos cantidades
      setStoreActive(llave, willBeActive, { clearQty: !willBeActive });

      renderTiendas(dom);
    });

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

    dom.tiendasContainer.addEventListener("focusin", (ev) => {
      const input = ev.target.closest(".detalle-qty-box");
      if (!input) return;
      try { input.select(); } catch {}
    });
    dom.tiendasContainer.addEventListener("input", (ev) => {
      const input = ev.target.closest(".detalle-qty-box");
      if (!input) return;

      const llave = norm(input.dataset.llave);
      const talla = norm(input.dataset.talla);
      if (!llave || !talla) return;

      // Si la tienda está apagada, ignoramos (por seguridad)
      if (!isStoreActive(llave)) return;

      // Convertimos a entero >= 0
      const raw = (input.value ?? "").toString().trim();
      let qty = parseInt(raw, 10);
      if (Number.isNaN(qty)) qty = 0;
      if (qty < 0) qty = 0;

      // Inicializamos si no existe
      if (!state.cantidades[llave]) state.cantidades[llave] = {};
      state.cantidades[llave][talla] = qty;

      // Actualiza los KPIs del footer en tiempo real
      updateFooterStats(dom);
    });
    dom.tiendasContainer.addEventListener("blur", (ev) => {
      const input = ev.target.closest(".detalle-qty-box");
      if (!input) return;

      const raw = (input.value ?? "").toString().trim();
      if (raw === "") input.value = "0";
    }, true);
    dom.tiendasContainer.addEventListener("keydown", (ev) => {
      const input = ev.target.closest(".detalle-qty-box");
      if (!input) return;

      if (ev.key === "Enter") {
        ev.preventDefault();

        // Lista de inputs habilitados en el modal (en el orden del DOM)
        const inputs = Array.from(dom.tiendasContainer.querySelectorAll(".detalle-qty-box:not([disabled])"));
        const idx = inputs.indexOf(input);

        if (idx >= 0 && idx < inputs.length - 1) {
          inputs[idx + 1].focus();
          inputs[idx + 1].select?.();
        }
      }
    });

    dom.btnCancelar?.addEventListener("click", () => {
      const modal = bootstrap.Modal.getInstance(dom.modalEl) || bootstrap.Modal.getOrCreateInstance(dom.modalEl);
      modal.hide();
    });

    // Limpiar = limpia filtros + cantidades + recarga
    dom.btnLimpiar?.addEventListener("click", async () => {
      limpiarFiltros(dom);
      await refetch();
    });

    dom.btnPresetSku?.addEventListener("click", () => {
      aplicarPresetDesdeSku();
      renderTiendas(dom);
    });

    dom.btnActivarTodas?.addEventListener("click", () => {
      setAllStoresActive(true);
      renderTiendas(dom);
    });

    dom.btnDesactivarTodas?.addEventListener("click", () => {
      setAllStoresActive(false, { clearQty: true });
      renderTiendas(dom);
    });

    dom.btnGuardar?.addEventListener("click", async () => {
      setError(dom, "");
      setLoading(dom, true);

      try {
        const resp = await guardarSegmentacion(dom);

        dom.footerInfoEl.textContent = `Guardado exitoso • ${new Date().toLocaleString()}`;

        // Dispara evento SOLO después de guardar OK
        window.dispatchEvent(new CustomEvent("segmentacion:guardada", {
          detail: {
            referenciaSku: state.ref.referenciaSku,
            apiResponse: resp
          }
        }));

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
