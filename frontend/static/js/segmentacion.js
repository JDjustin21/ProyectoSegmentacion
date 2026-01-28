  document.addEventListener("DOMContentLoaded", () => {
    console.log("JS de segmentación cargado");

    // =========================
    // 1) Leer configuración (no hardcode)
    // =========================
    const cardsContainer = document.getElementById("cards-container");
    const cardsPerPage = Number(cardsContainer?.dataset.cardsPerPage);
    const imagesBaseUrl = (cardsContainer?.dataset.imagesBaseUrl || "").trim();
    const placeholderUrl = (cardsContainer?.dataset.placeholderUrl || "https://via.placeholder.com/120x120?text=IMG").trim();

    if (!imagesBaseUrl) {
      throw new Error("Falta configuración: data-images-base-url en #cards-container");
    }

    if (!Number.isFinite(cardsPerPage) || cardsPerPage <= 0) {
      throw new Error("Configuración inválida: data-cards-per-page debe ser un número > 0 en #cards-container");
    }

    // =========================
    // 2) Leer dataset JSON (fuente de verdad)
    // =========================
    const dataScript = document.getElementById("data-referencias");
    if (!dataScript) {
      throw new Error("No se encontró el script #data-referencias con el JSON de referencias");
    }

    let referencias = [];
    try {
      referencias = JSON.parse(dataScript.textContent || "[]");
    } catch (e) {
      throw new Error("JSON inválido en #data-referencias");
    }

    // =========================
    // 3) Estado UI
    // =========================
    let currentPage = 1;

    const filtros = {
      portafolio: "",
      linea: "",
      estado: "",
      cuento: "",
      categoria: "",
      referencia: ""
    };

    // =========================
    // 4) Elementos UI
    // =========================
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

    let totalPages = 1;

    function norm(v) {
      return (v || "").toString().trim();
    }

    function normLower(v) {
      return norm(v).toLowerCase();
    }

    // =========================
    // 5) Filtro sobre DATA (no DOM)
    // =========================
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

    // =========================
    // 6) Actualizar selects (opciones dependientes)
    // =========================
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

      // Línea (dependiente de portafolio si está seleccionado)
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

    // =========================
    // 7) Actualizar datalists (categoría / referencia)
    // =========================
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

    // =========================
    // 8) Render de cards visibles (solo página actual)
    // =========================
    function crearCardHTML(ref) {
      // Nota: mantenemos estructura parecida a tu card original
      // y agregamos data para doble click (cantidadTallas, tallas).
      const referencia = norm(ref.referencia);
      const descripcion = norm(ref.descripcion);
      const categoria = norm(ref.categoria);
      const estado = norm(ref.estado);
      const cuento = norm(ref.cuento);
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
      const portafolio = norm(ref.tipoPortafolio);
      const linea = norm(ref.linea);
      const color = norm(ref.color);
      const codigoColor = norm(ref.codigoColor);

      const cantidadTallas = Number(ref.cantidadTallas || 0);
      const tallas = norm(ref.tallas);
      const imgUrl = `${imagesBaseUrl}/${encodeURIComponent(referencia)}`;


      return `
        <div class="card mb-3 shadow-sm reference-card"
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
        >
          <div class="row g-0 align-items-center">
            <div class="col-md-2 text-center p-2">
              <img
                src="${imgUrl}"
                class="img-fluid rounded"
                alt="Imagen referencia"
                onerror="this.onerror=null;this.src='${placeholderUrl}';">
            </div>

            <div class="col-md-4">
              <div class="card-body py-2">
                <h6 class="fw-bold mb-1">${referencia}</h6>
                <p class="mb-1 small text-muted">${descripcion}</p>
                <p class="mb-1 small"><strong>Categoría:</strong> ${categoria}</p>
                <span class="badge ${badgeClass} ${badgeTextClass}">${estado}</span>
              </div>
            </div>

            <div class="col-md-3">
              <div class="card-body py-2 border-start">
                <p class="mb-1 small"><strong>Portafolio:</strong> ${portafolio}</p>
                <p class="mb-1 small"><strong>Línea:</strong> ${linea}</p>
                <p class="mb-1 small"><strong>Color:</strong> ${color}</p>
                <p class="mb-1 small"><strong>Cuento:</strong> ${cuento}</p>
              </div>
            </div>

            <div class="col-md-3">
              <div class="card-body py-2 border-start">
                <p class="mb-1 small"><strong>Promedio Ventas:</strong> —</p>
                <p class="mb-1 small"><strong>CPD:</strong> —</p>
                <p class="mb-1 small"><strong>% Retorno:</strong> —</p>
                <p class="mb-0 small"><strong>Tiendas Activas:</strong> —</p>
              </div>
            </div>
          </div>
        </div>
      `;
    }

    function render() {
      const filtradas = aplicarFiltros();

      totalPages = Math.max(1, Math.ceil(filtradas.length / cardsPerPage));
      if (currentPage > totalPages) currentPage = 1;

      // Paginación
      const visibles = filtradas.slice((currentPage - 1) * cardsPerPage, currentPage * cardsPerPage);

      // Render cards (solo visibles)
      if (!cardsContainer) return;
      cardsContainer.innerHTML = visibles.map(crearCardHTML).join("");

      // Indicadores paginación
      if (pageIndicator) pageIndicator.textContent = `Página ${currentPage} de ${totalPages}`;
      if (prevBtn) prevBtn.disabled = currentPage === 1;
      if (nextBtn) nextBtn.disabled = currentPage === totalPages;

      // Opciones de filtros (según dataset filtrado)
      actualizarDatalists(filtradas);
      actualizarSelects(filtradas);
    }

    // =========================
    // 9) Eventos filtros
    // =========================
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

    // =========================
    // 10) Limpiar filtros
    // =========================
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

    // =========================
    // 11) Paginación
    // =========================
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

    // =========================
    // 12) Doble click (base)
    // =========================
    // Nota: como las cards ahora se crean dinámicamente, usamos delegación de eventos.
    cardsContainer?.addEventListener("dblclick", (event) => {
      const card = event.target.closest(".reference-card");
      if (!card) return;

      const ref = card.dataset.referencia;
      const cantidadTallas = card.dataset.cantidadtallas;
      const tallas = card.dataset.tallas;

      // Por ahora: solo log (luego lo conectamos a un modal real)
      console.log("Doble click referencia:", ref);
      console.log("Cantidad tallas:", cantidadTallas, "Tallas:", tallas);

      // Aquí en el siguiente paso abrimos tu modal
    });

    // Inicial
    render();
  });
