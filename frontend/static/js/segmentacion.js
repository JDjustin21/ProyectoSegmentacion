  // frontend/js/segmentacion.js
  document.addEventListener("DOMContentLoaded", () => {
    console.log("JS de segmentación cargado");

    // =========================================================
    // 1) Configuración desde el HTML (data-attributes)
    //    - Esto evita hardcode en JS.
    //    - El backend (Flask) es quien inyecta estos valores.
    // =========================================================
    const cardsContainer = document.getElementById("cards-container");

    const cardsPerPage = Number(cardsContainer?.dataset.cardsPerPage);
    const imagesBaseUrl = (cardsContainer?.dataset.imagesBaseUrl || "").trim();
    const placeholderUrl = (cardsContainer?.dataset.placeholderUrl || "https://via.placeholder.com/120x120?text=IMG").trim();

    const imageResolverUrl = (cardsContainer?.dataset.imageResolverUrl || "").trim();

    if (!imageResolverUrl) {
      throw new Error("Falta configuración: data-image-resolver-url en #cards-container");
    }
    // Config de tallas (fallbacks)
    const defaultTallasMvp = (cardsContainer?.dataset.defaultTallasMvp || "").trim();       // Ej: "XS,S,M,L,XL,XXL"
    const lineasTallasFijasRaw = (cardsContainer?.dataset.lineasTallasFijas || "").trim();  // Ej: "Dama Exterior;Dama Deportivo;..."

    if (!cardsContainer) {
      throw new Error("No se encontró #cards-container");
    }

    if (!imagesBaseUrl) {
      throw new Error("Falta configuración: data-images-base-url en #cards-container");
    }

    if (!Number.isFinite(cardsPerPage) || cardsPerPage <= 0) {
      throw new Error("Configuración inválida: data-cards-per-page debe ser un número > 0 en #cards-container");
    }

    // =========================================================
    // 2) Dataset JSON embebido (fuente de verdad para cards)
    // =========================================================
    const referenciasApiUrl = (cardsContainer?.dataset.referenciasApiUrl || "").trim();
    if (!referenciasApiUrl) {
      throw new Error("Falta configuración: data-referencias-api-url en #cards-container");
    }

    const referenciasDetalleApiUrl = `${referenciasApiUrl.replace(/\/$/, "")}/detalle`;

    let referencias = [];
    const referenciasByRef = new Map();
    const detalleByRef = new Map();

    async function cargarReferencias() {
      cardsContainer.innerHTML = `
        <div class="alert alert-light text-center w-100">
          Cargando referencias...
        </div>
      `;

      const res = await fetch(referenciasApiUrl, {
        headers: { "Accept": "application/json" }
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Error cargando referencias: HTTP ${res.status} - ${text}`);
      }

      const json = await res.json();
      if (!json.ok) {
        throw new Error(json.error || "No fue posible cargar referencias.");
      }

      referencias = Array.isArray(json.data) ? json.data : [];
      rebuildReferenciasIndex();
    }

    function rebuildReferenciasIndex() {
      referenciasByRef.clear();

      referencias.forEach((r) => {
        const key = norm(r?.referencia);
        if (key) {
          referenciasByRef.set(key, r);
        }
      });
    }

    function getReferenciaResumen(referenciaSku) {
      return referenciasByRef.get(norm(referenciaSku)) || null;
    }

    async function obtenerDetalleReferencia(referenciaSku) {
      const ref = norm(referenciaSku);
      if (!ref) {
        throw new Error("Referencia inválida para consultar detalle.");
      }

      if (detalleByRef.has(ref)) {
        return detalleByRef.get(ref);
      }

      const url = new URL(referenciasDetalleApiUrl, window.location.origin);
      url.searchParams.set("referenciaSku", ref);

      const res = await fetch(url.toString(), {
        headers: { "Accept": "application/json" }
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Error cargando detalle: HTTP ${res.status} - ${text}`);
      }

      const json = await res.json();
      if (!json.ok) {
        throw new Error(json.error || "No fue posible cargar el detalle de la referencia.");
      }

      const data = json.data || null;
      if (!data) {
        throw new Error("El backend no devolvió detalle para la referencia.");
      }

      detalleByRef.set(ref, data);
      return data;
    }

    function setReferenciaSegmentadaYConteo(referenciaSku, isSegmented, tiendasActivasSegmentadas) {
      const obj = getReferenciaResumen(referenciaSku);
      if (!obj) return;

      const flag = isSegmented === true;

      obj.is_segmented = flag;
      obj.isSegmented = flag;

      const n = Number(tiendasActivasSegmentadas);
      obj.tiendas_activas_segmentadas = Number.isFinite(n) ? n : 0;

      render();
    }


    // Evento que dispara detalle.js SOLO cuando guarda OK
    window.addEventListener("segmentacion:guardada", (ev) => {
      const ref = ev?.detail?.referenciaSku;
      if (!ref) return;

      const resp = ev?.detail?.apiResponse || {};

      // El backend te devuelve esto en resumen.is_segmented
      const isSeg = resp?.is_segmented;
      const count = resp?.tiendas_activas_segmentadas;

      if (isSeg === true || isSeg === false) {
        setReferenciaSegmentadaYConteo(ref, isSeg, count);
      }
    });

    // =========================================================
    // 3) Helpers (funciones puras, sin tocar DOM)
    // =========================================================
    function norm(v) {
      return (v || "").toString().trim();
    }

    function normLower(v) {
      return norm(v).toLowerCase();
    }

    function toBool(v) {
    if (v === true) return true;
    if (v === false) return false;

    // números
    if (typeof v === "number") return v !== 0;

    const s = (v ?? "").toString().trim().toLowerCase();
    if (!s) return false;

    // variantes comunes que llegan desde Postgres/JSON
    if (["true", "t", "1", "yes", "y", "si", "sí"].includes(s)) return true;
    if (["false", "f", "0", "no", "n"].includes(s)) return false;

    // ojo: cualquier otro string no lo asumimos true para no “colar” cosas raras
    return false;
  }

  function isRefSegmentada(r) {
    // 1) si viene un flag explícito
    const flag =
      toBool(r?.is_segmented) ||
      toBool(r?.isSegmented) ||
      toBool(r?.is_segmentada) ||
      toBool(r?.isSegmentada);

    if (flag) return true;

    // 2) si no viene flag o viene raro, usamos el conteo (lo más confiable en tu caso)
    const n = Number(r?.tiendas_activas_segmentadas ?? r?.tiendasActivasSegmentadas ?? 0);
    if (Number.isFinite(n) && n > 0) return true;

    return false;
  }

    // "S, M, L" -> ["S","M","L"]
    // "10, 12, 14" -> ["10","12","14"]
    // "12/18, 18/24" -> ["12/18","18/24"]
    function parseCSVList(value) {
      if (Array.isArray(value)) {
        return value
          .map(x => norm(x))
          .filter(Boolean);
      }

      return (value || "")
        .split(",")
        .map(x => x.trim())
        .filter(Boolean);
    }

    // "A;B;C" -> ["A","B","C"]
    function parseSemiColonList(str) {
      return (str || "")
        .split(";")
        .map(x => x.trim())
        .filter(Boolean);
    }

    function normalizarLineaTexto(lineaRaw) {
      const v = norm(lineaRaw);
      if (!v) return "";

      // Quita prefijo numérico con guion, tolerando espacios variables
      // Ej: "12 - Hombre Exterior" / "12- Hombre Exterior" / "12  -  Hombre Exterior"
      const sinPrefijo = v.replace(/^\s*\d+\s*-\s*/g, "");

      // Compacta espacios múltiples por seguridad (evita "Hombre   Exterior")
      return sinPrefijo.replace(/\s+/g, " ").trim();
    }

    

    // =========================================================
    // 4) Config derivada (arrays) para reglas de tallas
    // =========================================================
    const defaultTallasArray = parseCSVList(defaultTallasMvp);             // fallback principal
    const lineasFijasArray = parseSemiColonList(lineasTallasFijasRaw);     // líneas que usan tallas fijas (fallback por línea)

    // =========================================================
    // 5) Estado UI (paginación y filtros)
    // =========================================================
    let currentPage = 1;

    const filtros = {
      portafolio: "",
      linea: "",
      estado: "",
      cuento: "",
      categoria: "",
      referencia: "",
      soloSegmentadas: false,
      soloNoSegmentadas: false
    };

    // =========================================================
    // 6) Elementos UI
    // =========================================================
    const filtroPortafolio = document.getElementById("filtroPortafolio");
    const filtroLinea = document.getElementById("filtroLinea");
    const filtroEstado = document.getElementById("filtroEstado");
    const filtroCuento = document.getElementById("filtroCuento");
    const filtroCategoria = document.getElementById("filtroCategoria");
    const filtroReferencia = document.getElementById("filtroReferencia");
    const filtroSoloSegmentadas = document.getElementById("filtroSoloSegmentadas");
    const filtroSoloNoSegmentadas = document.getElementById("filtroSoloNoSegmentadas");

    const listaCategorias = document.getElementById("listaCategorias");
    const listaCuentos = document.getElementById("listaCuentos");
    const listaReferencias = document.getElementById("listaReferencias");

    const prevBtn = document.getElementById("prevPage");
    const nextBtn = document.getElementById("nextPage");
    const pageIndicator = document.getElementById("pageIndicator");

    const btnLimpiar = document.getElementById("btnLimpiar");
    const btnCopiarTienda = document.getElementById("btnCopiarTienda");
    const copyStoreMenu = document.getElementById("copyStoreMenu");
    const copyStoreWrap = document.getElementById("copyStoreWrap");

    // “inicio de sesión” de la página (para exportar por rango desde que abriste la pantalla)
    const pageSessionStartIso = new Date().toISOString();

    let totalPages = 1;
    let tiendasCandidatasCopiar = [];

    // =========================================================
    // 7) Filtro sobre DATA (no DOM)
    // =========================================================
    function aplicarFiltros() {
      const catFiltro = normLower(filtros.categoria);
      const refFiltro = normLower(filtros.referencia);

      return referencias.filter(r => {
        const port = norm(r.tipoPortafolio);
        const lin = norm(r.linea);
        const est = norm(r.estado);
        const cue = norm(r.cuento);
        const cat = normLower(r.categoria);
        const ref = normLower(r.referencia);

        if (filtros.portafolio && port !== norm(filtros.portafolio)) return false;
        if (filtros.linea && lin !== norm(filtros.linea)) return false;
        if (filtros.estado && est !== norm(filtros.estado)) return false;
        const cueFiltro = normLower(filtros.cuento);
        const cueNorm = normLower(cue);

        if (cueFiltro) {
          // Si el usuario escribió el cuento completo, funciona “igual que antes” (exacto)
          if (cueNorm === cueFiltro) {
            // ok exacto
          } else {
            // Mientras escribe, hacemos búsqueda parcial para que filtre en tiempo real
            if (!cueNorm.includes(cueFiltro)) return false;
          }
        }

        if (catFiltro && !cat.includes(catFiltro)) return false;
        if (refFiltro && !ref.includes(refFiltro)) return false;

        const estaSegmentada = isRefSegmentada(r);
        if (filtros.soloSegmentadas === true && !estaSegmentada) return false;
        if (filtros.soloNoSegmentadas === true && estaSegmentada) return false;

        return true;
      });
    }

    // =========================================================
    // 8) Actualizar selects (opciones dependientes)
    // =========================================================
    function actualizarSelects(datasetFiltrado) {
      // Portafolio
      const portafolios = [...new Set(datasetFiltrado.map(r => norm(r.tipoPortafolio)).filter(Boolean))].sort();

      if (filtroPortafolio) {
        const valorActual = norm(filtros.portafolio);
        filtroPortafolio.innerHTML = `<option value="">Todos</option>`;
        portafolios.forEach(p => {
          const opt = document.createElement("option");
          opt.value = p;
          opt.textContent = p;
          if (p === valorActual) opt.selected = true;
          filtroPortafolio.appendChild(opt);
        });
      }

      // Línea
      const baseLineas = filtros.portafolio
        ? datasetFiltrado.filter(r => norm(r.tipoPortafolio) === norm(filtros.portafolio))
        : datasetFiltrado;

      const lineas = [...new Set(baseLineas.map(r => norm(r.linea)).filter(Boolean))].sort();

      if (filtroLinea) {
        const valorActual = norm(filtros.linea);
        filtroLinea.innerHTML = `<option value="">Todas</option>`;
        lineas.forEach(l => {
          const opt = document.createElement("option");
          opt.value = l;
          opt.textContent = l;
          if (l === valorActual) opt.selected = true;
          filtroLinea.appendChild(opt);
        });
      }

      // Estado
      const estados = [...new Set(datasetFiltrado.map(r => norm(r.estado)).filter(Boolean))].sort();

      if (filtroEstado) {
        const valorActual = norm(filtros.estado);
        filtroEstado.innerHTML = `<option value="">Todos</option>`;
        estados.forEach(e => {
          const opt = document.createElement("option");
          opt.value = e;
          opt.textContent = e;
          if (e === valorActual) opt.selected = true;
          filtroEstado.appendChild(opt);
        });
      }

      // Cuento
      const cuentos = [...new Set(datasetFiltrado.map(r => norm(r.cuento)).filter(Boolean))].sort();

      if (listaCuentos) {
        listaCuentos.innerHTML = "";
        cuentos.forEach(c => {
          const opt = document.createElement("option");
          opt.value = c;
          listaCuentos.appendChild(opt);
        });
      }
  }

    // =========================================================
    // 9) Actualizar datalists (categoría / referencia)
    // =========================================================
    function actualizarDatalists(datasetFiltrado) {
      const categorias = [...new Set(datasetFiltrado.map(r => norm(r.categoria)).filter(Boolean))].sort();
      const refs = [...new Set(datasetFiltrado.map(r => norm(r.referencia)).filter(Boolean))].sort();

      if (listaCategorias) {
        listaCategorias.innerHTML = "";
        categorias.forEach(c => {
          const opt = document.createElement("option");
          opt.value = c;
          listaCategorias.appendChild(opt);
        });
      }

      if (listaReferencias) {
        listaReferencias.innerHTML = "";
        refs.forEach(r => {
          const opt = document.createElement("option");
          opt.value = r;
          listaReferencias.appendChild(opt);
        });
      }
    }

    // =========================================================
    // 10) Render cards (solo página actual)
    // =========================================================

    function render() {
      const filtradas = aplicarFiltros();

      totalPages = Math.max(1, Math.ceil(filtradas.length / cardsPerPage));
      if (currentPage > totalPages) currentPage = 1;

      const visibles = filtradas.slice((currentPage - 1) * cardsPerPage, currentPage * cardsPerPage);

      const cardConfig = { imageResolverUrl, placeholderUrl };
      cardsContainer.innerHTML = visibles
        .map(r => window.CardReferencia.crearCardHTML(r, cardConfig))
        .join("");


      if (pageIndicator) pageIndicator.textContent = `Página ${currentPage} de ${totalPages}`;
      if (prevBtn) prevBtn.disabled = currentPage === 1;
      if (nextBtn) nextBtn.disabled = currentPage === totalPages;

      actualizarDatalists(filtradas);
      actualizarSelects(filtradas);
    }

    // =========================================================
    // 11) Eventos filtros
    // =========================================================
    filtroPortafolio?.addEventListener("change", e => {
      filtros.portafolio = norm(e.target.value);
      filtros.linea = "";
      filtros.categoria = "";
      filtros.referencia = "";
      currentPage = 1;
      render();
    });

    filtroLinea?.addEventListener("change", async e => {
      filtros.linea = norm(e.target.value);
      console.log("[COPIAR TIENDA] línea seleccionada:", filtros.linea);

      filtros.categoria = "";
      filtros.referencia = "";
      currentPage = 1;
      render();

      try {
        await cargarTiendasCandidatasCopiar();
        console.log("[COPIAR TIENDA] candidatas:", tiendasCandidatasCopiar);
        console.log("[COPIAR TIENDA] botón disabled:", btnCopiarTienda?.disabled);
      } catch (err) {
        console.error("Error cargando tiendas candidatas:", err);
      }
    });

    filtroEstado?.addEventListener("change", e => {
      filtros.estado = norm(e.target.value);
      currentPage = 1;
      render();
    });

    filtroCuento?.addEventListener("input", e => {
      filtros.cuento = norm(e.target.value);
      currentPage = 1;
      render();
    });

    filtroCategoria?.addEventListener("input", e => {
      filtros.categoria = norm(e.target.value).toLowerCase();
      currentPage = 1;
      render();
    });

    filtroReferencia?.addEventListener("input", e => {
      filtros.referencia = norm(e.target.value).toLowerCase();
      currentPage = 1;
      render();
    });

    filtroSoloSegmentadas?.addEventListener("change", e => {
      filtros.soloSegmentadas = e.target.checked === true;
      currentPage = 1;
      render();
    });

    filtroSoloNoSegmentadas?.addEventListener("change", e => {
      filtros.soloNoSegmentadas = e.target.checked === true;

      // si activo no segmentadas, apago segmentadas
      if (filtros.soloNoSegmentadas) {
        filtros.soloSegmentadas = false;
        if (filtroSoloSegmentadas) filtroSoloSegmentadas.checked = false;
      }

      currentPage = 1;
      render();
    });

    btnLimpiar?.addEventListener("click", () => {
      filtros.portafolio = "";
      filtros.linea = "";
      filtros.estado = "";
      filtros.cuento = "";
      filtros.categoria = "";
      filtros.referencia = "";
      filtros.soloSegmentadas = false;
      filtros.soloNoSegmentadas = false;

      if (filtroPortafolio) filtroPortafolio.value = "";
      if (filtroLinea) filtroLinea.value = "";
      if (filtroEstado) filtroEstado.value = "";
      if (filtroCuento) filtroCuento.value = "";
      if (filtroCategoria) filtroCategoria.value = "";
      if (filtroReferencia) filtroReferencia.value = "";
      if (filtroSoloSegmentadas) filtroSoloSegmentadas.checked = false;
      if (filtroSoloNoSegmentadas) filtroSoloNoSegmentadas.checked = false;

      currentPage = 1;
      render();
      tiendasCandidatasCopiar = [];
      renderMenuCopiarTiendas();
    });

    function buildQuery(params) {
      const usp = new URLSearchParams();
      Object.entries(params).forEach(([k, v]) => {
        const value = (v || "").toString().trim();
        if (value) usp.set(k, value);
      });
      return usp.toString();
    }

    const API_TIENDAS_CANDIDATAS_COPIAR = "/segmentacion/api/segmentaciones/tiendas-candidatas";
    const API_PREVIEW_COPIAR_TIENDA = "/segmentacion/api/segmentaciones/copiar-tienda/preview";
    const API_EJECUTAR_COPIAR_TIENDA = "/segmentacion/api/segmentaciones/copiar-tienda/ejecutar";

    async function fetchJson(url, options = {}) {
      const res = await fetch(url, {
        headers: { "Accept": "application/json", ...(options.headers || {}) },
        ...options
      });

      const json = await res.json().catch(() => null);

      if (!res.ok) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }
      return json;
    }

    async function cargarTiendasCandidatasCopiar() {
      const linea = norm(filtros.linea);

      tiendasCandidatasCopiar = [];

      if (!linea) {
        renderMenuCopiarTiendas();
        return;
      }

      const url = `${API_TIENDAS_CANDIDATAS_COPIAR}?${buildQuery({ linea })}`;
      const json = await fetchJson(url);

      tiendasCandidatasCopiar = Array.isArray(json.data) ? json.data : [];
      renderMenuCopiarTiendas();
    }

    function renderMenuCopiarTiendas() {
      if (!btnCopiarTienda || !copyStoreMenu || !copyStoreWrap) return;

      copyStoreMenu.innerHTML = "";

      const linea = norm(filtros.linea);
      const hasLinea = !!linea;
      const items = Array.isArray(tiendasCandidatasCopiar) ? tiendasCandidatasCopiar : [];

      if (!hasLinea) {
        btnCopiarTienda.disabled = true;
        copyStoreMenu.innerHTML = `
          <div class="preset-menu-item">
            <div style="font-size:13px; font-weight:500;">Selecciona una línea</div>
            <div style="font-size:11px; opacity:0.65;">Debes filtrar primero por línea comercial</div>
          </div>
        `;
        return;
      }

      btnCopiarTienda.disabled = false;

      if (items.length === 0) {
        copyStoreMenu.innerHTML = `
          <div class="preset-menu-item">
            <div style="font-size:13px; font-weight:500;">Sin tiendas disponibles</div>
            <div style="font-size:11px; opacity:0.65;">No hay tiendas segmentadas para copiar en esta línea</div>
          </div>
        `;
        return;
      }

      items.forEach(item => {
        const refEjemplo = norm(item.referencia_ejemplo);
        const llave = norm(item.llave_naval);
        const tienda = norm(item.desc_dependencia || item.dependencia);
        const ciudad = norm(item.ciudad);
        const refs = Number(item.referencias_segmentadas || 0);
        const und = Number(item.total_unidades || 0);

        const el = document.createElement("div");
        el.className = "preset-menu-item copy-store-item";
        el.dataset.llaveNaval = llave;

        el.innerHTML = `
          <div style="font-size:13px; font-weight:500;">
            ${tienda || llave}
          </div>
          <div style="font-size:11px; opacity:0.65;">
            ${[ciudad, `${refs} referencias`, `${und} und`].filter(Boolean).join(" • ")}
          </div>
          ${
            refEjemplo
              ? `<div style="font-size:10px; opacity:0.5;">Referencia Origen: ${refEjemplo}</div>`
              : ""
          }
        `;

        copyStoreMenu.appendChild(el);
      });
    }

    async function previewCopiarTienda(llaveNavalOrigen) {
      const payload = {
        linea: norm(filtros.linea),
        llave_naval_origen: norm(llaveNavalOrigen)
      };

      return fetchJson(API_PREVIEW_COPIAR_TIENDA, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    async function ejecutarCopiarTienda(llaveNavalOrigen) {
      const payload = {
        linea: norm(filtros.linea),
        llave_naval_origen: norm(llaveNavalOrigen),
        confirmar: true
      };

      return fetchJson(API_EJECUTAR_COPIAR_TIENDA, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    function exportHoyGlobal() {
      const today = new Date();
      const yyyy = today.getFullYear();
      const mm = String(today.getMonth() + 1).padStart(2, "0");
      const dd = String(today.getDate()).padStart(2, "0");
      const fecha = `${yyyy}-${mm}-${dd}`;

      window.location.href = `/segmentacion/api/export/csv?${buildQuery({ fecha })}`;
    }

    function exportSesionGlobal() {
      const desde = pageSessionStartIso;
      const hasta = new Date().toISOString();
      window.location.href = `/segmentacion/api/export/csv?${buildQuery({ desde, hasta })}`;
    }

    document.addEventListener("DOMContentLoaded", () => {
    const btnExportAll = document.getElementById("btnExportAllGlobal");
    if (btnExportAll) {
      btnExportAll.addEventListener("click", () => {
        // Exporta TODO lo guardado (Postgres)
        window.location.href = "/segmentacion/api/segmentaciones/export/excel";
      });
    }
  });

  btnCopiarTienda?.addEventListener("click", (e) => {
    e.stopPropagation();
    if (!copyStoreMenu || btnCopiarTienda.disabled) return;

    copyStoreMenu.style.display =
      copyStoreMenu.style.display === "block" ? "none" : "block";
  });

  copyStoreMenu?.addEventListener("click", async (e) => {
    const item = e.target.closest(".copy-store-item");
    if (!item) return;

    const llaveNavalOrigen = norm(item.dataset.llaveNaval);
    if (!llaveNavalOrigen) return;

    copyStoreMenu.style.display = "none";

    try {
      const preview = await previewCopiarTienda(llaveNavalOrigen);
      if (!preview.ok) {
        alert(preview.error || "No fue posible previsualizar la copia.");
        return;
      }

      const refs = Number(preview.referencias_afectadas || 0);
      const linea = norm(preview.linea);
      const refOrigen = norm(preview.referencia_origen);

      const confirmado = window.confirm(
        `Se va a copiar la segmentación de la tienda seleccionada a ${refs} referencias segmentadas de la línea "${linea}".` +
        `${refOrigen ? `\n\nReferencia origen: ${refOrigen}` : ""}` +
        `\n\n¿Estás seguro que quieres guardar la segmentación?`
      );

      if (!confirmado) return;

      const result = await ejecutarCopiarTienda(llaveNavalOrigen);
      if (!result.ok) {
        alert(result.error || "No fue posible ejecutar la copia de tienda.");
        return;
      }

      alert(
        `Proceso completado.\n` +
        `Referencias actualizadas: ${result.referencias_actualizadas || 0}\n` +
        `Referencias omitidas: ${result.referencias_omitidas || 0}`
      );

      await cargarReferencias();
      render();

      try {
        await cargarTiendasCandidatasCopiar();
      } catch (err) {
        console.error("Error recargando candidatas:", err);
      }

    } catch (err) {
      console.error(err);
      alert(err?.message || "Error procesando la copia de tienda.");
    }
  });

  document.addEventListener("click", (e) => {
    if (!copyStoreWrap?.contains(e.target)) {
      if (copyStoreMenu) {
        copyStoreMenu.style.display = "none";
      }
    }
  });
    
    // =========================================================
    // 12) Paginación
    // =========================================================
    prevBtn?.addEventListener("click", () => {
      if (currentPage > 1) {
        currentPage--;
        render();
      }
    });

    nextBtn?.addEventListener("click", () => {
      if (currentPage < totalPages) {
        currentPage++;
        render();
      }
    });

    document.getElementById("btnExportSegmentaciones")?.addEventListener("click", () => {
    window.location.href = "/segmentacion/api/export/csv";
  });

    // =========================================================
    // 13) Doble click (MVP: resolver tallas finales)
    // =========================================================
      cardsContainer.addEventListener("dblclick", async (event) => {
      const card = event.target.closest(".reference-card");
        if (!card) return;

        const referenciaSku = norm(card.dataset.referencia);
        if (!referenciaSku) {
          console.warn("La card no tiene referencia.");
          return;
        }

        if (!window.SegmentacionDetalle || typeof window.SegmentacionDetalle.open !== "function") {
          console.error("detalle.js no está cargado o no expone SegmentacionDetalle.open()");
          return;
        }

        const resumen = getReferenciaResumen(referenciaSku);
        if (!resumen) {
          console.warn("No se encontró el resumen de la referencia en memoria:", referenciaSku);
          return;
        }

        // Datos livianos del resumen
        const descripcion = norm(resumen.descripcion);
        const categoria = norm(resumen.categoria);
        const estado = norm(resumen.estado);
        const tipoPortafolio = norm(resumen.tipoPortafolio);
        const precioUnitario = norm(resumen.precioUnitario);
        const color = norm(resumen.color);
        const cuento = norm(resumen.cuento);

        let detalle = null;

        try {
          detalle = await obtenerDetalleReferencia(referenciaSku);
        } catch (err) {
          console.error(err);
          alert(`No fue posible cargar el detalle de la referencia ${referenciaSku}.`);
          return;
        }

        const lineaRaw = norm(detalle?.linea || resumen.linea);
        const lineaTexto = normalizarLineaTexto(lineaRaw);

        const tallasFromSku = parseCSVList(detalle?.tallas);
        let tallasFinal = tallasFromSku;

        if (
          tallasFinal.length === 0 &&
          lineaTexto &&
          lineasFijasArray.includes(lineaTexto) &&
          defaultTallasArray.length > 0
        ) {
          tallasFinal = defaultTallasArray;
        }

        if (!lineaTexto) {
          console.warn("Referencia sin línea. No se puede consultar tiendas para el modal.");
          return;
        }

        if (tallasFinal.length === 0) {
          console.warn("Referencia sin tallas y sin fallback aplicable. Revisa configuración o datos del SKU.");
          return;
        }

        const tallasConteo = detalle?.tallasConteo || null;
        const codigoBarras = norm(detalle?.codigoBarras || detalle?.codigo_barras || "");
        const codigosBarrasPorTalla =
          detalle?.codigosBarrasPorTalla ||
          detalle?.codigos_barras_por_talla ||
          null;

        const tipoInventario = norm(detalle?.tipoInventario || detalle?.tipo_inventario || "");

        const referenciaBase = norm(
          detalle?.referenciaBase ||
          detalle?.referencia_base ||
          ""
        );

        const codigoColor = norm(
          detalle?.codigoColor ||
          detalle?.codigo_color ||
          ""
        );

        const perfilPrenda = norm(
          detalle?.perfilPrenda ||
          detalle?.perfil_prenda ||
          ""
        );

        fetch("/segmentacion/api/referencias/ack", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ referencia: referenciaSku })
        }).catch(() => {});

        const dot = card.querySelector(".dot-new");
        if (dot) dot.remove();

        window.SegmentacionDetalle.open({
          referenciaSku,
          referenciaBase,
          codigoColor,
          color,
          perfilPrenda,

          descripcion,
          categoria,
          estado,
          tipoPortafolio,
          precioUnitario,
          lineaRaw,
          lineaTexto,
          cuento,
          tallasFinal,
          tallasConteo,
          codigoBarras,
          codigosBarrasPorTalla,
          tipoInventario
        });
      });

    async function init() {
      try {
        await cargarReferencias();
        render();
        await cargarTiendasCandidatasCopiar();
      } catch (err) {
        console.error(err);
        cardsContainer.innerHTML = `
          <div class="alert alert-danger w-100">
            No fue posible cargar las referencias. ${err?.message || ""}
          </div>
        `;
      }
    }

    // Inicial
    init();
  });
