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
  const dataScript = document.getElementById("data-referencias");
  if (!dataScript) {
    throw new Error("No se encontró #data-referencias con el JSON de referencias");
  }

  let referencias = [];
  try {
    referencias = JSON.parse(dataScript.textContent || "[]");
  } catch {
    throw new Error("JSON inválido en #data-referencias");
  }

  // =========================================================
  // 3) Helpers (funciones puras, sin tocar DOM)
  // =========================================================
  function norm(v) {
    return (v || "").toString().trim();
  }

  function normLower(v) {
    return norm(v).toLowerCase();
  }

  // "S, M, L" -> ["S","M","L"]
  // "10, 12, 14" -> ["10","12","14"]
  // "12/18, 18/24" -> ["12/18","18/24"]
  function parseCSVList(str) {
    return (str || "")
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
    referencia: ""
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

  const listaCategorias = document.getElementById("listaCategorias");
  const listaReferencias = document.getElementById("listaReferencias");

  const prevBtn = document.getElementById("prevPage");
  const nextBtn = document.getElementById("nextPage");
  const pageIndicator = document.getElementById("pageIndicator");

  const btnLimpiar = document.getElementById("btnLimpiar");

  // “inicio de sesión” de la página (para exportar por rango desde que abriste la pantalla)
  const pageSessionStartIso = new Date().toISOString();

  let totalPages = 1;

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
      if (filtros.cuento && cue !== norm(filtros.cuento)) return false;

      if (catFiltro && !cat.includes(catFiltro)) return false;
      if (refFiltro && !ref.includes(refFiltro)) return false;

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

    if (filtroCuento) {
      const valorActual = norm(filtros.cuento);
      filtroCuento.innerHTML = `<option value="">Todos</option>`;
      cuentos.forEach(c => {
        const opt = document.createElement("option");
        opt.value = c;
        opt.textContent = c;
        if (c === valorActual) opt.selected = true;
        filtroCuento.appendChild(opt);
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
  function crearCardHTML(ref) {
    const referencia = norm(ref.referencia);
    const categoria = norm(ref.categoria);
    const estado = norm(ref.estado);
    const linea = norm(ref.linea);
    const cuento = norm(ref.cuento);

    const descripcion = norm(ref.descripcion);
    const portafolio = norm(ref.tipoPortafolio);
    const color = norm(ref.color);
    const codigoColor = norm(ref.codigoColor);

    const cantidadTallas = Number(ref.cantidadTallas || 0);
    const tallas = norm(ref.tallas);

    const estadoLower = estado.toLowerCase();
    let badgeClass = "bg-secondary";
    let badgeTextClass = "";

    if (estadoLower === "activo") {
      badgeClass = "bg-success";
    } else if (estadoLower === "inactivo") {
      badgeClass = "bg-danger";
    } else if (estadoLower === "moda") {
      badgeClass = "bg-warning";
      badgeTextClass = "text-dark";
    }

    const imgUrl = `${imagesBaseUrl}/${encodeURIComponent(referencia)}`;

    return `
      <div class="product-card reference-card"
        data-referencia="${referencia}"
        data-descripcion="${descripcion}"
        data-categoria="${categoria}"
        data-estado="${estado}"
        data-portafolio="${portafolio}"
        data-linea="${linea}"
        data-color="${color}"
        data-codigocolor="${codigoColor}"
        data-cantidadtallas="${cantidadTallas}"
        data-tallas="${tallas}"
        data-cuento="${cuento}"
      >
        <div class="product-image">
          <img
            src="${imgUrl}"
            alt="Imagen referencia"
            onerror="this.onerror=null;this.src='${placeholderUrl}';">
        </div>

        <div class="product-info">

          <div class="product-info-head">
            <div class="product-ref">${referencia}</div>
            <span class="badge ${badgeClass} ${badgeTextClass}">${estado || "—"}</span>
          </div>

          <div class="product-meta">
            <div><b>Categoría:</b> ${categoria || "—"}</div>
            <div><b>Línea:</b> ${linea || "—"}</div>
            <div><b>Cuento:</b> ${cuento || "—"}</div>
          </div>
        </div>
      </div>
    `;
  }

  function render() {
    const filtradas = aplicarFiltros();

    totalPages = Math.max(1, Math.ceil(filtradas.length / cardsPerPage));
    if (currentPage > totalPages) currentPage = 1;

    const visibles = filtradas.slice((currentPage - 1) * cardsPerPage, currentPage * cardsPerPage);

    cardsContainer.innerHTML = visibles.map(crearCardHTML).join("");

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

  filtroLinea?.addEventListener("change", e => {
    filtros.linea = norm(e.target.value);
    filtros.categoria = "";
    filtros.referencia = "";
    currentPage = 1;
    render();
  });

  filtroEstado?.addEventListener("change", e => {
    filtros.estado = norm(e.target.value);
    currentPage = 1;
    render();
  });

  filtroCuento?.addEventListener("change", e => {
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

  btnLimpiar?.addEventListener("click", () => {
    filtros.portafolio = "";
    filtros.linea = "";
    filtros.estado = "";
    filtros.cuento = "";
    filtros.categoria = "";
    filtros.referencia = "";

    if (filtroPortafolio) filtroPortafolio.value = "";
    if (filtroLinea) filtroLinea.value = "";
    if (filtroEstado) filtroEstado.value = "";
    if (filtroCuento) filtroCuento.value = "";
    if (filtroCategoria) filtroCategoria.value = "";
    if (filtroReferencia) filtroReferencia.value = "";

    currentPage = 1;
    render();
  });

  function buildQuery(params) {
    const usp = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      const value = (v || "").toString().trim();
      if (value) usp.set(k, value);
    });
    return usp.toString();
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
    cardsContainer.addEventListener("dblclick", (event) => {
    const card = event.target.closest(".reference-card");
    if (!card) return;

    // 1) Datos del SKU (desde dataset de la card)
    const referenciaSku = norm(card.dataset.referencia);
    const lineaRaw = norm(card.dataset.linea);
    const tallasStr = norm(card.dataset.tallas);

    const descripcion = norm(card.dataset.descripcion);
    const categoria = norm(card.dataset.categoria);
    const estado = norm(card.dataset.estado);
    const tipoPortafolio = norm(card.dataset.portafolio);
    const color = norm(card.dataset.color);
    const cuento = norm(card.dataset.cuento);

    // Si luego agregas más data-* (codigoBarras, tipoInventario, tallasConteo), lo pasas aquí también.
    // Para tallasConteo, si no lo tienes en data-*, puedes buscarlo en el array "referencias" por referenciaSku.

    // 2) Normalización y regla tallas
    const lineaTexto = normalizarLineaTexto(lineaRaw);
    const tallasFromSku = parseCSVList(tallasStr);

    let tallasFinal = tallasFromSku;

    // Fallback: solo aplica a líneas fijas (las 4 grandes) y si hay default configurado
    if (tallasFinal.length === 0 && lineaTexto && lineasFijasArray.includes(lineaTexto) && defaultTallasArray.length > 0) {
      tallasFinal = defaultTallasArray;
    }

    // 3) Validaciones mínimas antes de abrir modal
    if (!lineaTexto) {
      console.warn("Referencia sin línea. No se puede consultar tiendas para el modal.");
      return;
    }

    if (tallasFinal.length === 0) {
      console.warn("Referencia sin tallas y sin fallback aplicable. Revisa configuración o datos del SKU.");
      return;
    }

    // 4) Abrir modal (delegamos a detalle.js)
    console.log("[DOBLECLICK] lineaRaw:", lineaRaw);
    console.log("[DOBLECLICK] lineaTexto:", lineaTexto);

    if (!window.SegmentacionDetalle || typeof window.SegmentacionDetalle.open !== "function") {
      console.error("detalle.js no está cargado o no expone SegmentacionDetalle.open()");
      return;
    }

    const refObj = referencias.find(r => norm(r.referencia) === referenciaSku);
    const tallasConteo = refObj?.tallasConteo || null;
    const codigoBarras = norm(refObj?.codigoBarras);
    const tipoInventario = norm(refObj?.tipoInventario);

    window.SegmentacionDetalle.open({
      referenciaSku,
      descripcion,
      categoria,
      estado,
      tipoPortafolio,
      lineaRaw,
      lineaTexto,
      color,
      cuento,
      tallasFinal,
      tallasConteo,
      codigoBarras,
      tipoInventario
    });
  });

  // Inicial
  render();
});
