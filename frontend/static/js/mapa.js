// frontend/static/js/mapa.js
//
// Controla el módulo de Mapa.
// Responsabilidades:
// - Leer URLs del backend desde mapa.html mediante data-*.
// - Inicializar Leaflet y sus capas.
// - Consultar filtros y datos del mapa.
// - Renderizar puntos de calor, marcadores y tabla de detalle.
// - Aplicar filtros manuales e interacción tipo Power BI.
// - Ordenar tablas de ventas y segmentación.


document.addEventListener("DOMContentLoaded", () => {
  const botonesTipo = document.querySelectorAll(".mapa-tipo-btn");

  const mapaEstado = document.getElementById("mapaEstado");
  const mapaTitulo = document.getElementById("mapaTitulo");
  const mapaTotalPuntos = document.getElementById("mapaTotalPuntos");
  const mapaTotalValor = document.getElementById("mapaTotalValor");

  const filtroMes = document.getElementById("filtroMes");
  const filtroLinea = document.getElementById("filtroLinea");
  const filtroCliente = document.getElementById("filtroCliente");
  const filtroCiudad = document.getElementById("filtroCiudad");
  const filtroTipoPortafolio = document.getElementById("filtroTipoPortafolio");
  const filtroEstado = document.getElementById("filtroEstado");
  const btnLimpiarFiltros = document.getElementById("btnLimpiarFiltros");

  const mapaTablaTitulo = document.getElementById("mapaTablaTitulo");
  const mapaTablaHead = document.getElementById("mapaTablaHead");
  const mapaTablaBody = document.getElementById("mapaTablaBody");
  const mapaTablaCard = document.getElementById("mapaTablaCard");
  const app = document.querySelector(".mapa-page");

  const apiFiltrosUrl = app?.dataset.apiFiltrosUrl || "/mapa/api/filtros";
  const apiDatosUrl = app?.dataset.apiDatosUrl || "/mapa/api/datos";

  let mapa = null;
  let capaCalor = null;
  let capaMarcadores = null;
  let tipoActual = "ventas";
  let tablaBaseActual = [];

  let ordenTabla = {
    columna: null,
    direccion: "desc",
  };

  inicializarPeriodo();
  inicializarMapa();
  configurarEventos();
  actualizarVisibilidadPeriodo();

  cargarFiltros()
    .then(() => cargarDatosMapa(tipoActual))
    .catch((error) => {
      console.error("[MAPA][INIT][ERROR]", error);
      mostrarEstado("No fue posible inicializar el módulo de mapa.");
    });

  function inicializarPeriodo() {
    const fechaActual = new Date();
    const mesActual = fechaActual.getMonth() + 1;

    filtroMes.value = String(mesActual);
  }

  function inicializarMapa() {
    const boundsColombia = L.latLngBounds(
      [-4.5, -79.2],
      [13.8, -66.8]
    );

    mapa = L.map("mapaContenedor", {
      maxBounds: boundsColombia,
      maxBoundsViscosity: 0.9,
      minZoom: 5,
      maxZoom: 10,
    });

    mapa.fitBounds(boundsColombia, {
      padding: [10, 10],
    });

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(mapa);

    capaMarcadores = L.layerGroup().addTo(mapa);
  }

  function configurarEventos() {
    botonesTipo.forEach((boton) => {
        boton.addEventListener("click", () => {
          tipoActual = boton.dataset.tipo;

          botonesTipo.forEach((b) => {
            b.classList.remove("active", "btn-primary");
            b.classList.add("btn-outline-primary");
          });

          boton.classList.add("active", "btn-primary");
          boton.classList.remove("btn-outline-primary");

          actualizarVisibilidadPeriodo();
          cargarDatosMapa(tipoActual);
        });
      });

    [
      filtroMes,
      filtroLinea,
      filtroCliente,
      filtroCiudad,
      filtroTipoPortafolio,
      filtroEstado,
    ].forEach((control) => {
      control.addEventListener("change", () => {

        cargarDatosMapa(tipoActual);
      });
    });

    btnLimpiarFiltros.addEventListener("click", () => {
      filtroLinea.value = "";
      filtroCliente.value = "";
      filtroCiudad.value = "";
      filtroTipoPortafolio.value = "";
      filtroEstado.value = "";
      cargarDatosMapa(tipoActual);
    });
  }

  function actualizarVisibilidadPeriodo() {
    const mostrarPeriodo = tipoActual === "ventas" || tipoActual === "segmentacion";

    filtroMes.closest("div").style.display = mostrarPeriodo ? "" : "none";
  }

  function alternarValorSelect(select, valor) {
    const valorActual = String(select.value || "").trim();
    const valorNuevo = String(valor || "").trim();

    select.value = valorActual === valorNuevo ? "" : valorNuevo;
  }

  async function cargarFiltros() {
    const response = await fetch(apiFiltrosUrl);

    if (!response.ok) {
      throw new Error("No fue posible consultar los filtros.");
    }

    const payload = await response.json();

    if (!payload.ok) {
      throw new Error(payload.error || "No fue posible cargar filtros.");
    }

    llenarSelect(filtroLinea, payload.data.lineas || [], "Todas");
    llenarSelect(filtroCliente, payload.data.clientes || [], "Todos");
    llenarSelect(filtroCiudad, payload.data.ciudades || [], "Todas");
    llenarSelect(filtroTipoPortafolio, payload.data.tipos_portafolio || [], "Todos");
    llenarSelect(filtroEstado, payload.data.estados || [], "Todos");
  }

  function llenarSelect(select, valores, textoTodos) {
    select.innerHTML = "";

    const optionTodos = document.createElement("option");
    optionTodos.value = "";
    optionTodos.textContent = textoTodos;
    select.appendChild(optionTodos);

    valores.forEach((valor) => {
      const option = document.createElement("option");
      option.value = valor;
      option.textContent = valor;
      select.appendChild(option);
    });
  }

  function obtenerFiltrosActuales() {
    return {
      lineas: valorComoLista(filtroLinea.value),
      clientes: valorComoLista(filtroCliente.value),
      ciudades: valorComoLista(filtroCiudad.value),
      tipos_portafolio: valorComoLista(filtroTipoPortafolio.value),
      estados: valorComoLista(filtroEstado.value),
    };
  }

  function valorComoLista(valor) {
    const limpio = String(valor || "").trim();
    return limpio ? [limpio] : [];
  }

  async function cargarDatosMapa(tipo) {
    mostrarEstado("Cargando información del mapa...");

    try {
      const body = {
        tipo,
        filtros: obtenerFiltrosActuales(),
      };

      if (tipo === "ventas" || tipo === "segmentacion") {
        body.anio = new Date().getFullYear();
        body.mes = Number(filtroMes.value);
      }

      const response = await fetch(apiDatosUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error("Respuesta no válida del servidor.");
      }

      const payload = await response.json();

      if (!payload.ok) {
        throw new Error(payload.error || "No fue posible cargar el mapa.");
      }

      renderizarMapa(payload.data, payload.meta);
      renderizarTabla(payload.data);
    } catch (error) {
      console.error("[MAPA][ERROR]", error);
      limpiarCapas();
      limpiarTabla("No fue posible cargar la información.");
      mostrarEstado("No fue posible cargar la información del mapa.");
    }
  }

  function renderizarMapa(data, meta) {
    const puntos = data.puntos || [];

    mapaTitulo.textContent = data.titulo || "Mapa";
    mapaTotalPuntos.textContent = String(meta.total_ciudades || 0);
    mapaTotalValor.textContent = formatearTotal(data.tipo, meta.total_valor || 0);

    limpiarCapas();

    if (!puntos.length) {
      mostrarEstado("No hay ciudades con coordenadas para esta vista.");
      return;
    }

    ocultarEstado();

    const maxValor = Math.max(...puntos.map((p) => Number(p.valor || 0)), 1);

    const puntosCalor = puntos.map((p) => {
      const intensidad = Number(p.valor || 0) / maxValor;
      return [Number(p.latitud), Number(p.longitud), intensidad];
    });

    capaCalor = L.heatLayer(puntosCalor, {
      radius: 34,
      blur: 24,
      maxZoom: 12,
    }).addTo(mapa);

    puntos.forEach((punto) => {
      const color = obtenerColorCalor(punto.valor, maxValor);

      const marcador = L.circleMarker([punto.latitud, punto.longitud], {
        radius: calcularRadio(punto.valor, maxValor),
        color: color,
        fillColor: color,
        weight: 1,
        fillOpacity: 0.75,
      });

      marcador.bindPopup(crearPopupCiudad(punto, data.unidad));

      marcador.on("click", () => {
        alternarValorSelect(filtroCiudad, punto.ciudad || "");
        cargarDatosMapa(tipoActual);
      });

      marcador.addTo(capaMarcadores);
    });

    ajustarVista(puntos);
  }

  function renderizarTabla(data) {
    const filas = data.tabla || [];
    tablaBaseActual = filas;

    if (data.tipo === "inventario") {
      mapaTablaCard.style.display = "none";
      return;
    }

    mapaTablaCard.style.display = "";

    if (data.tipo === "ventas") {
      mapaTablaTitulo.textContent = "Ventas clientes VMI";
      renderizarTablaVentas(aplicarOrden(tablaBaseActual));
      return;
    }

    mapaTablaTitulo.textContent = "Segmentación por ciudad";
    renderizarTablaSegmentacion(aplicarOrden(tablaBaseActual));
  }

  function renderizarTablaVentas(filas) {
    const tablaOrdenada = aplicarOrden(filas);

    mapaTablaHead.innerHTML = `
      <tr>
        ${encabezadoOrdenable("Línea", "linea")}
        ${encabezadoOrdenable("Venta Unds<br>N-1", "venta_unds_anterior", "text-end")}
        ${encabezadoOrdenable("Venta<br>Unds", "venta_unds", "text-end")}
        ${encabezadoOrdenable("% Part", "participacion", "text-end")}
        ${encabezadoOrdenable("% Var N-1", "variacion_anterior", "text-end")}
        ${encabezadoOrdenable("# Tiendas", "tiendas", "text-end")}
        ${encabezadoOrdenable("Prom Unds<br>x Tienda", "promedio_unds_tienda", "text-end")}
      </tr>
    `;

    mapaTablaBody.innerHTML = tablaOrdenada.map((fila) => {
      const esTotal = fila.linea === "Total";
      const claseActiva = filtroLinea.value === fila.linea ? "mapa-fila-activa" : "";
      const claseFila = esTotal ? "mapa-fila-total" : `mapa-fila-click ${claseActiva}`;
      const variacion = Number(fila.variacion_anterior || 0);

      return `
        <tr class="${claseFila}" data-linea="${escaparHtml(fila.linea || "")}">
          <td>${escaparHtml(fila.linea || "")}</td>
          <td class="text-end">${formatearNumero(fila.venta_unds_anterior)}</td>
          <td class="text-end">${formatearNumero(fila.venta_unds)}</td>
          <td class="text-end">${formatearPorcentaje(fila.participacion)}</td>
          <td class="text-end">${renderizarVariacion(variacion)}</td>
          <td class="text-end">${formatearNumero(fila.tiendas)}</td>
          <td class="text-end">${formatearNumero(Math.round(Number(fila.promedio_unds_tienda || 0)))}</td>
        </tr>
      `;
    }).join("");

    mapaTablaBody.querySelectorAll(".mapa-fila-click").forEach((tr) => {
      tr.addEventListener("click", () => {
        const linea = tr.dataset.linea || "";

        if (!linea || linea === "Total") {
          return;
        }

        alternarValorSelect(filtroLinea, linea);
        cargarDatosMapa(tipoActual);
      });
    });

    activarOrdenamientoTabla();
  }

  function renderizarTablaSegmentacion(filas) {
    const tablaOrdenada = aplicarOrden(filas);

    mapaTablaHead.innerHTML = `
      <tr>
        ${encabezadoOrdenable("Ciudad", "ciudad")}
        ${encabezadoOrdenable("Línea", "linea")}
        ${encabezadoOrdenable("Cliente", "cliente")}
        ${encabezadoOrdenable("Tipo<br>portafolio", "tipo_portafolio")}
        ${encabezadoOrdenable("Estado", "estado_sku")}
        ${encabezadoOrdenable("Cantidad<br>segmentada", "cantidad_segmentada", "text-end")}
        ${encabezadoOrdenable("Tiendas", "tiendas", "text-end")}
        ${encabezadoOrdenable("Refs.", "referencias_segmentadas", "text-end")}
        ${encabezadoOrdenable("Prom. segmentado<br>x tienda", "promedio_segmentado_tienda", "text-end")}
      </tr>
    `;

    mapaTablaBody.innerHTML = tablaOrdenada.map((fila) => {
      const claseActiva = filtroLinea.value === fila.linea ? "mapa-fila-activa" : "";

      return `
        <tr class="mapa-fila-click ${claseActiva}" data-linea="${escaparHtml(fila.linea || "")}">
          <td>${escaparHtml(fila.ciudad || "")}</td>
          <td>${escaparHtml(fila.linea || "")}</td>
          <td>${escaparHtml(fila.cliente || "")}</td>
          <td>${escaparHtml(fila.tipo_portafolio || "")}</td>
          <td>${escaparHtml(fila.estado_sku || "")}</td>
          <td class="text-end">${formatearNumero(fila.cantidad_segmentada)}</td>
          <td class="text-end">${formatearNumero(fila.tiendas)}</td>
          <td class="text-end">${formatearNumero(fila.referencias_segmentadas)}</td>
          <td class="text-end">${formatearNumero(Math.round(Number(fila.promedio_segmentado_tienda || 0)))}</td>
        </tr>
      `;
    }).join("");

    mapaTablaBody.querySelectorAll(".mapa-fila-click").forEach((tr) => {
      tr.addEventListener("click", () => {
        const linea = tr.dataset.linea || "";

        if (!linea) return;

        alternarValorSelect(filtroLinea, linea);
        cargarDatosMapa(tipoActual);
      });
    });

    activarOrdenamientoTabla();
  }

  function limpiarTabla(mensaje) {
    mapaTablaHead.innerHTML = "";
    mapaTablaBody.innerHTML = `
      <tr>
        <td>${escaparHtml(mensaje)}</td>
      </tr>
    `;
  }

  function limpiarCapas() {
    if (capaCalor) {
      mapa.removeLayer(capaCalor);
      capaCalor = null;
    }

    if (capaMarcadores) {
      capaMarcadores.clearLayers();
    }
  }

  function ajustarVista(puntos) {
    const bounds = puntos.map((p) => [p.latitud, p.longitud]);

    if (bounds.length > 0) {
      mapa.fitBounds(bounds, {
        padding: [30, 30],
      });
    }
  }

  function calcularRadio(valor, maxValor) {
    const base = 6;
    const extra = 18 * (Number(valor || 0) / maxValor);

    return base + extra;
  }

  function obtenerColorCalor(valor, maxValor) {
    const proporcion = Number(valor || 0) / Math.max(Number(maxValor || 1), 1);

    if (proporcion >= 0.75) {
      return "#b91c1c";
    }

    if (proporcion >= 0.50) {
      return "#f97316";
    }

    if (proporcion >= 0.25) {
      return "#facc15";
    }

    return "#22c55e";
  }

  function crearPopupCiudad(punto, unidad) {
    return `
      <strong>${escaparHtml(punto.ciudad || "Ciudad sin nombre")}</strong><br>
      Departamento: ${escaparHtml(punto.departamento || "Sin departamento")}<br>
      Valor: ${formatearNumero(punto.valor)} ${escaparHtml(unidad || "")}<br>
      Tiendas: ${formatearNumero(punto.tiendas || 0)}
    `;
  }

  function renderizarVariacion(valor) {
    const porcentaje = formatearPorcentaje(valor);
    const clase = valor < 0 ? "mapa-var-negativa" : valor > 0 ? "mapa-var-positiva" : "mapa-var-neutra";
    const simbolo = valor < 0 ? "▼" : valor > 0 ? "▲" : "●";

    return `<span class="${clase}">${simbolo} ${porcentaje}</span>`;
  }

  function mostrarEstado(mensaje) {
    mapaEstado.textContent = mensaje;
    mapaEstado.style.display = "block";
  }

  function ocultarEstado() {
    mapaEstado.style.display = "none";
  }

  function formatearTotal(tipo, valor) {
    if (tipo === "ventas") {
      return `${formatearNumero(valor)} unds`;
    }

    if (tipo === "inventario") {
      return `${formatearNumero(valor)} exist.`;
    }

    return `${formatearNumero(valor)} seg.`;
  }

  function formatearNumero(valor) {
    return new Intl.NumberFormat("es-CO", {
      maximumFractionDigits: 0,
    }).format(Number(valor || 0));
  }

  function aplicarOrden(filas) {
    if (!ordenTabla.columna) {
      return [...filas];
    }

    const direccion = ordenTabla.direccion === "asc" ? 1 : -1;
    const columna = ordenTabla.columna;

    return [...filas].sort((a, b) => {
      if (a.linea === "Total") return 1;
      if (b.linea === "Total") return -1;

      const valorA = a[columna];
      const valorB = b[columna];

      const numA = Number(valorA);
      const numB = Number(valorB);

      if (!Number.isNaN(numA) && !Number.isNaN(numB)) {
        return (numA - numB) * direccion;
      }

      return String(valorA || "").localeCompare(String(valorB || ""), "es") * direccion;
    });
  }

  function encabezadoOrdenable(label, columna, clase = "") {
    const indicador = ordenTabla.columna === columna
      ? (ordenTabla.direccion === "asc" ? " ▲" : " ▼")
      : "";

    return `
      <th class="${clase}" data-sort="${columna}" role="button">
        <span class="mapa-th-label">${label}</span>
        <span class="mapa-th-sort">${indicador}</span>
      </th>
    `;
  }

  function activarOrdenamientoTabla() {
    mapaTablaHead.querySelectorAll("[data-sort]").forEach((th) => {
      th.addEventListener("click", () => {
        const columna = th.dataset.sort;

        if (ordenTabla.columna === columna) {
          ordenTabla.direccion = ordenTabla.direccion === "asc" ? "desc" : "asc";
        } else {
          ordenTabla.columna = columna;
          ordenTabla.direccion = "desc";
        }

        if (tipoActual === "ventas") {
          renderizarTablaVentas(aplicarOrden(tablaBaseActual));
          return;
        }

        renderizarTablaSegmentacion(aplicarOrden(tablaBaseActual));
      });
    });
  }

  function formatearPorcentaje(valor) {
    return `${new Intl.NumberFormat("es-CO", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 1,
    }).format(Number(valor || 0) * 100)} %`;
  }

  function escaparHtml(valor) {
    return String(valor)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }
});