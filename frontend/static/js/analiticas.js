// frontend/static/js/analiticas.js
(() => {
  "use strict";

  document.addEventListener("DOMContentLoaded", () => {
    console.log("JS de analíticas cargado");

    const app = document.getElementById("analiticasAgotadosApp");

    if (!app) {
      throw new Error("No se encontró #analiticasAgotadosApp");
    }

    const apiDashboardUrl = (app.dataset.apiDashboardUrl || "").trim();

    if (!apiDashboardUrl) {
      throw new Error("Falta configuración: data-api-dashboard-url en #analiticasAgotadosApp");
    }

    const apiRefreshBaseUrl = (app.dataset.apiRefreshBaseUrl || "").trim();

    if (!apiRefreshBaseUrl) {
      throw new Error("Falta configuración: data-api-refresh-base-url en #analiticasAgotadosApp");
    }

    const $ = (selector) => document.querySelector(selector);

    const state = {
      loading: false,
      lastData: null,
      tableSort: {
        referencias: { key: "", dir: "asc" },
        tiendas: { key: "", dir: "asc" },
        detalle: { key: "", dir: "asc" },
      },
    };

    const charts = {
      linea: null,
      talla: null,
      zona: null,
      clasificacion: null,
    };

    // =========================================================
    // 1) Eventos principales
    // =========================================================
    bindEventos();
    cargarDashboardAgotados();

    function bindEventos() {

      document.querySelectorAll("input[name='filtroClaseAgotados']").forEach((radio) => {
        radio.addEventListener("change", () => {
          cargarDashboardAgotados();
        });
      });

      document.addEventListener("click", (event) => {
        const sortableHeader = event.target.closest("th[data-sort-table][data-sort-key]");

        if (sortableHeader) {
          event.preventDefault();

          const tableKey = sortableHeader.dataset.sortTable || "";
          const sortKey = sortableHeader.dataset.sortKey || "";

          aplicarOrdenTabla(tableKey, sortKey);
          return;
        }

        const row = event.target.closest(".clickable-row");
        if (!row) return;

        const tipo = row.dataset.filtroTipo || "";
        const valor = row.dataset.filtroValor || "";

        if (!valor) return;

        if (tipo === "referencia") {
          aplicarFiltroDesdeGrafico("#filtroReferenciaAgotados", valor);
        }

        if (tipo === "tienda") {
          aplicarFiltroDesdeGrafico("#filtroTiendaAgotados", valor);
        }
      });
    
      $("#btnActualizarAgotados")?.addEventListener("click", () => {
        cargarDashboardAgotados();
      });

      $("#btnLimpiarFiltrosAgotados")?.addEventListener("click", () => {
        limpiarFiltros();
        cargarDashboardAgotados();
      });

      $("#btnRefrescarBaseAgotados")?.addEventListener("click", async () => {
        await refrescarBaseAgotados();
      });

      const refetchDebounced = debounce(() => {
        cargarDashboardAgotados();
      }, 350);

      const filtrosInput = [
        "#filtroLineaAgotados",
        "#filtroCuentoAgotados",
        "#filtroReferenciaAgotados",
        "#filtroClienteAgotados",
        "#filtroTiendaAgotados",
        "#filtroTipoPortafolioAgotados",
        "#filtroZonaAgotados",
        "#filtroClasificacionAgotados",
      ];

      filtrosInput.forEach((selector) => {
        const input = $(selector);
        if (!input) return;

        input.addEventListener("input", refetchDebounced);

        input.addEventListener("keydown", (event) => {
          if (event.key === "Enter") {
            cargarDashboardAgotados();
          }
        });
      });
    }

    // =========================================================
    // 2) Carga de datos
    // =========================================================
    async function cargarDashboardAgotados() {
      if (state.loading) return;

      state.loading = true;
      setLoading(true);

      try {
        const json = await fetchJson(apiDashboardUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(obtenerFiltros()),
        });

        state.lastData = json;
        pintarDashboard(json.data || {}, json.meta || {});
      } catch (error) {
        console.error("[ANALITICAS][AGOTADOS]", error);
        mostrarError(error.message || "Error cargando analíticas.");
      } finally {
        state.loading = false;
        setLoading(false);
      }
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

      if (!res.ok) {
        throw new Error(
          json?.detalle ||
          json?.error ||
          `HTTP ${res.status}`
        );
      }

      if (!json?.ok) {
        throw new Error(
          json?.detalle ||
          json?.error ||
          "La respuesta del backend no fue exitosa."
        );
      }

      return json;
    }

    async function refrescarBaseAgotados() {
      if (state.loading) return;

      const confirmar = window.confirm(
        "Esto recalculará la base de agotados. Puede tardar algunos segundos. ¿Deseas continuar?"
      );

      if (!confirmar) return;

      state.loading = true;
      setLoading(true, "Refrescando base...");

      try {
        const json = await fetchJson(apiRefreshBaseUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({}),
        });

        console.log("[ANALITICAS][REFRESH_BASE]", json.data);

        await cargarDashboardAgotados();
      } catch (error) {
        console.error("[ANALITICAS][REFRESH_BASE]", error);
        mostrarError(error.message || "Error refrescando base de agotados.");
      } finally {
        state.loading = false;
        setLoading(false);
      }
    }

    // =========================================================
    // 3) Filtros
    // =========================================================
   function obtenerFiltros() {
      const claseAgotados =
        document.querySelector("input[name='filtroClaseAgotados']:checked")?.value || "";

      return {
        linea: valor("#filtroLineaAgotados"),
        cuento: valor("#filtroCuentoAgotados"),
        referencia_sku: valor("#filtroReferenciaAgotados"),
        cliente: valor("#filtroClienteAgotados"),
        dependencia: valor("#filtroTiendaAgotados"),
        tipo_portafolio: valor("#filtroTipoPortafolioAgotados"),
        clase_agotados: claseAgotados,
        zona: valor("#filtroZonaAgotados"),
        clasificacion: valor("#filtroClasificacionAgotados"),
      };
    }

    function valor(selector) {
      return ($(selector)?.value || "").trim();
    }

    function limpiarFiltros() {
      [
        "#filtroLineaAgotados",
        "#filtroCuentoAgotados",
        "#filtroReferenciaAgotados",
        "#filtroClienteAgotados",
        "#filtroTiendaAgotados",
        "#filtroTipoPortafolioAgotados",
        "#filtroZonaAgotados",
        "#filtroClasificacionAgotados",
      ].forEach((selector) => {
        const input = $(selector);
        if (input) input.value = "";
      });

      const claseTodos = $("#filtroClaseTodos");
      if (claseTodos) claseTodos.checked = true;
    }

    // =========================================================
    // 4) Pintar dashboard
    // =========================================================
    function pintarDashboard(data, meta) {
      const kpis = data.kpis || {};

      setText("#kpiReferenciasSegmentadas", formatoNumero(kpis.total_referencias_sku));
      setText("#kpiTallasSegmentadas", formatoNumero(kpis.total_tallas_segmentadas));
      setText("#kpiTallasAgotadas", formatoNumero(kpis.total_tallas_agotadas));
      setText("#kpiPorcentajeAgotado", formatoPorcentaje(kpis.porcentaje_agotado_tallas));
      setText("#kpiTiendasAgotado", formatoNumero(kpis.total_puntos_venta_con_agotado));

      const registros = meta.registros_base ?? 0;
      const regla = meta.regla_agotado || "";

      setText(
        "#agotadosMetaTexto",
        `${formatoNumero(registros)} registros · ${regla}`
      );


      pintarGraficoBarrasInteractivo({
        chartKey: "linea",
        selector: "#chartAgotadosLinea",
        rows: data.por_linea || [],
        campo: "linea",
        filtroSelector: "#filtroLineaAgotados",
      });

      pintarGraficoBarrasInteractivo({
        chartKey: "talla",
        selector: "#chartAgotadosTalla",
        rows: data.por_talla || [],
        campo: "talla",
        filtroSelector: null,
      });

      pintarGraficoDonutInteractivo({
        chartKey: "zona",
        selector: "#chartAgotadosZona",
        rows: data.por_zona || [],
        campo: "zona",
        filtroSelector: "#filtroZonaAgotados",
      });

      pintarGraficoDonutInteractivo({
        chartKey: "clasificacion",
        selector: "#chartAgotadosClasificacion",
        rows: data.por_clasificacion || [],
        campo: "clasificacion",
        filtroSelector: "#filtroClasificacionAgotados",
      });

      
      pintarReferenciasConAgotados(
        data.referencias_con_agotados || data.top_referencias || []
      );
      pintarTiendas(data.por_tienda || []);
      pintarDetalle(data.detalle || []);

      actualizarDatalistsAgotados(data);
    }

    function aplicarOrdenTabla(tableKey, sortKey) {
      if (!state.tableSort[tableKey]) return;

      const current = state.tableSort[tableKey];

      if (current.key === sortKey) {
        current.dir = current.dir === "asc" ? "desc" : "asc";
      } else {
        current.key = sortKey;
        current.dir = "asc";
      }

      repintarTabla(tableKey);
      actualizarIndicadoresOrden();
    }

    function repintarTabla(tableKey) {
      const data = state.lastData?.data || {};

      if (tableKey === "referencias") {
        pintarReferenciasConAgotados(
          data.referencias_con_agotados || data.top_referencias || []
        );
        return;
      }

      if (tableKey === "tiendas") {
        pintarTiendas(data.por_tienda || []);
        return;
      }

      if (tableKey === "detalle") {
        pintarDetalle(data.detalle || []);
        return;
      }
    }

    function ordenarRows(rows, tableKey) {
      const sort = state.tableSort[tableKey];

      if (!sort || !sort.key) {
        return Array.isArray(rows) ? [...rows] : [];
      }

      const dirFactor = sort.dir === "desc" ? -1 : 1;

      return [...rows].sort((a, b) => {
        const av = obtenerValorOrden(a, sort.key);
        const bv = obtenerValorOrden(b, sort.key);

        return compararValores(av, bv) * dirFactor;
      });
    }

    function obtenerValorOrden(row, key) {
      if (!row) return "";

      if (key === "punto_venta") {
        return (
          row.desc_dependencia ||
          row.dependencia ||
          row.punto_venta ||
          row.llave_naval ||
          row.tienda ||
          ""
        );
      }

      if (key === "cantidad_segmentada") {
        return row.cantidad_segmentada ?? row.total_segmentado ?? 0;
      }

      if (key === "cantidad_agotada") {
        return row.cantidad_agotada ?? row.total_agotado ?? 0;
      }

      if (key === "total_segmentado") {
        return row.total_segmentado ?? row.cantidad_segmentada ?? 0;
      }

      if (key === "total_agotado") {
        return row.total_agotado ?? row.cantidad_agotada ?? 0;
      }

      if (key === "color") {
        return row.color || row.codigo_color || "";
      }

      return row[key] ?? "";
    }

    function compararValores(a, b) {
      const na = convertirNumeroSeguro(a);
      const nb = convertirNumeroSeguro(b);

      if (na !== null && nb !== null) {
        return na - nb;
      }

      return String(a ?? "")
        .trim()
        .localeCompare(String(b ?? "").trim(), "es", {
          numeric: true,
          sensitivity: "base",
        });
    }

    function convertirNumeroSeguro(value) {
      if (value === null || value === undefined || value === "") {
        return null;
      }

      const n = Number(value);

      return Number.isFinite(n) ? n : null;
    }

    function actualizarIndicadoresOrden() {
      document.querySelectorAll("th[data-sort-table][data-sort-key]").forEach((th) => {
        const tableKey = th.dataset.sortTable || "";
        const sortKey = th.dataset.sortKey || "";
        const sort = state.tableSort[tableKey];

        th.classList.remove("sort-asc", "sort-desc", "sort-active");

        if (sort && sort.key === sortKey) {
          th.classList.add("sort-active");
          th.classList.add(sort.dir === "asc" ? "sort-asc" : "sort-desc");
        }
      });
    }

    function pintarBarras(selector, rows, campo) {
      const cont = $(selector);
      if (!cont) return;

      if (!Array.isArray(rows) || rows.length === 0) {
        cont.innerHTML = `<div class="text-muted text-center py-4">Sin datos</div>`;
        return;
      }

      const top = rows.slice(0, 4);
      const max = Math.max(...top.map((r) => Number(r.total_agotado || 0)), 1);

      cont.innerHTML = top.map((row) => {
        const etiqueta = escapeHtml(row[campo] || "Sin clasificar");
        const agotado = Number(row.total_agotado || 0);
        const segmentado = Number(row.total_segmentado || 0);
        const pct = Number(row.porcentaje_agotado || 0);
        const width = Math.max((agotado / max) * 100, 4);

        return `
          <div class="bar-row">
            <div class="bar-info">
              <span class="bar-label" title="${etiqueta}">${etiqueta}</span>
              <span class="bar-value">${formatoPorcentaje(pct)} · ${formatoNumero(agotado)}/${formatoNumero(segmentado)}</span>
            </div>
            <div class="bar-track">
              <div class="bar-fill" style="width:${width}%"></div>
            </div>
          </div>
        `;
      }).join("");
    }

    function pintarGraficoBarrasInteractivo({ chartKey, selector, rows, campo, filtroSelector }) {
      const el = $(selector);
      if (!el) return;

      if (!window.ApexCharts) {
        el.innerHTML = `<div class="text-danger text-center py-4">ApexCharts no está cargado.</div>`;
        return;
      }

      const dataRows = Array.isArray(rows) ? rows.slice(0, 6) : [];

      if (dataRows.length === 0) {
        destruirChart(chartKey);
        el.innerHTML = `<div class="text-muted text-center py-4">Sin datos</div>`;
        return;
      }

      const categorias = dataRows.map(row => row[campo] || "Sin clasificar");

      // La barra mide porcentaje, no cantidad absoluta.
      const porcentajes = dataRows.map(row => Number(row.porcentaje_agotado || 0));
      const agotados = dataRows.map(row => Number(row.total_agotado || 0));
      const segmentados = dataRows.map(row => Number(row.total_segmentado || 0));

      destruirChart(chartKey);

      const options = {
        chart: {
          type: "bar",
          height: 185,
          toolbar: {
            show: false,
          },
          events: {
            dataPointSelection: function (_event, _chartContext, config) {
              const index = config.dataPointIndex;
              const valorFiltro = categorias[index];
              aplicarFiltroDesdeGrafico(filtroSelector, valorFiltro);
            },
          },
        },
        series: [
          {
            name: "% agotado",
            data: porcentajes,
          },
        ],
        plotOptions: {
          bar: {
            horizontal: true,
            borderRadius: 5,
            barHeight: "55%",
            distributed: false,
          },
        },
        dataLabels: {
          enabled: true,
          formatter: function (value) {
            return `${Number(value || 0).toFixed(1)}%`;
          },
          style: {
            fontSize: "10px",
            fontWeight: 800,
            colors: ["#ffffff"],
          },
        },
        xaxis: {
          min: 0,
          max: 100,
          categories: categorias,
          labels: {
            show: false,
          },
          axisBorder: {
            show: false,
          },
          axisTicks: {
            show: false,
          },
        },
        yaxis: {
          labels: {
            style: {
              fontSize: "11px",
              fontWeight: 700,
            },
            maxWidth: 165,
          },
        },
        tooltip: {
          custom: function ({ dataPointIndex }) {
            const nombre = escapeHtml(categorias[dataPointIndex]);
            const pct = porcentajes[dataPointIndex].toFixed(2);
            const agot = formatoNumero(agotados[dataPointIndex]);
            const seg = formatoNumero(segmentados[dataPointIndex]);

            return `
              <div class="chart-tooltip-custom">
                <div class="fw-bold mb-1">${nombre}</div>
                <div>${pct}% agotado</div>
                <div>${agot} agotados de ${seg} segmentados</div>
              </div>
            `;
          },
        },
        grid: {
          borderColor: "#edf0f2",
          strokeDashArray: 3,
        },
        colors: ["#dc4451"],
      };

      charts[chartKey] = new ApexCharts(el, options);
      charts[chartKey].render();
    }

    function pintarGraficoDonutInteractivo({ chartKey, selector, rows, campo, filtroSelector }) {
      const el = $(selector);
      if (!el) return;

      if (!window.ApexCharts) {
        el.innerHTML = `<div class="text-danger text-center py-4">ApexCharts no está cargado.</div>`;
        return;
      }

      const dataRows = Array.isArray(rows)
        ? rows.filter(row => Number(row.total_agotado || 0) > 0).slice(0, 7)
        : [];

      if (dataRows.length === 0) {
        destruirChart(chartKey);
        el.innerHTML = `<div class="text-muted text-center py-4">Sin datos</div>`;
        return;
      }

      const labels = dataRows.map(row => row[campo] || "Sin clasificar");
      const series = dataRows.map(row => Number(row.total_agotado || 0));
      const porcentajes = dataRows.map(row => Number(row.porcentaje_agotado || 0));
      const segmentados = dataRows.map(row => Number(row.total_segmentado || 0));

      destruirChart(chartKey);

      const options = {
        chart: {
          type: "donut",
          height: 185,
          toolbar: {
            show: false,
          },
          events: {
            dataPointSelection: function (_event, _chartContext, config) {
              const index = config.dataPointIndex;
              const valorFiltro = labels[index];

              aplicarFiltroDesdeGrafico(filtroSelector, valorFiltro);
            },
          },
        },
        labels,
        series,
        legend: {
          position: "right",
          fontSize: "10px",
          fontWeight: 700,
          markers: {
            width: 8,
            height: 8,
          },
          itemMargin: {
            vertical: 2,
          },
        },
        dataLabels: {
          enabled: true,
          formatter: function (_val, opts) {
            const index = opts.seriesIndex;
            return `${porcentajes[index].toFixed(0)}%`;
          },
          style: {
            fontSize: "10px",
            fontWeight: 800,
          },
          dropShadow: {
            enabled: false,
          },
        },
        tooltip: {
          y: {
            formatter: function (value, opts) {
              const index = opts.seriesIndex;
              return `${formatoNumero(value)} agotados de ${formatoNumero(segmentados[index])} segmentados`;
            },
          },
        },
        plotOptions: {
          pie: {
            donut: {
              size: "58%",
              labels: {
                show: true,
                total: {
                  show: true,
                  label: "Agot.",
                  formatter: function (w) {
                    const total = w.globals.seriesTotals.reduce((a, b) => a + b, 0);
                    return formatoNumero(total);
                  },
                },
              },
            },
          },
        },
        stroke: {
          width: 2,
          colors: ["#ffffff"],
        },
        colors: ["#3f8f3a", "#2f3a78", "#b59a1e", "#8c1d2b", "#5b9bd5", "#7c2aa8", "#7f8c8d"],
      };

      charts[chartKey] = new ApexCharts(el, options);
      charts[chartKey].render();
    }

    function destruirChart(chartKey) {
      if (charts[chartKey]) {
        charts[chartKey].destroy();
        charts[chartKey] = null;
      }
    }

    function aplicarFiltroDesdeGrafico(selector, value) {
      if (!selector) return;

      const input = $(selector);
      if (!input) return;

      input.value = value || "";
      cargarDashboardAgotados();
    }

    function pintarReferenciasConAgotados(rows) {
      const tbody =
        $("#tablaReferenciasConAgotados")

      if (!tbody) return;

      if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = filaVacia(6);
        return;
      }

      const rowsOrdenadas = ordenarRows(rows, "referencias");

      tbody.innerHTML = rowsOrdenadas.map((row) => {
        const referencia = row.referencia_sku || "";
        const descripcion = row.descripcion || "";
        const color = row.color || row.codigo_color || "";
        const cantidadSegmentada = row.cantidad_segmentada ?? row.total_segmentado ?? 0;
        const cantidadAgotada = row.cantidad_agotada ?? row.total_agotado ?? 0;
        const porcentaje = row.porcentaje_agotado ?? 0;

        return `
          <tr class="clickable-row" data-filtro-tipo="referencia" data-filtro-valor="${escapeHtml(referencia)}">
            <td class="fw-semibold">${escapeHtml(referencia)}</td>
            <td>${escapeHtml(descripcion)}</td>
            <td>${escapeHtml(color)}</td>
            <td class="text-end">${formatoNumero(cantidadSegmentada)}</td>
            <td class="text-end text-danger fw-semibold">${formatoNumero(cantidadAgotada)}</td>
            <td class="text-end">${formatoPorcentaje(porcentaje)}</td>
          </tr>
        `;
      }).join("");
    }

    function pintarTiendas(rows) {
        const tbody = $("#tablaTiendasAgotados");
        if (!tbody) return;

        if (!Array.isArray(rows) || rows.length === 0) {
            tbody.innerHTML = filaVacia(4);
            return;
        }

        const rowsOrdenadas = ordenarRows(rows, "tiendas");

        tbody.innerHTML = rowsOrdenadas.slice(0, 20).map((row) => {
            const nombreTienda =
            row.desc_dependencia ||
            row.dependencia ||
            row.punto_venta ||
            row.llave_naval ||
            row.tienda ||
            "Sin tienda";

            return `
            <tr class="clickable-row" data-filtro-tipo="tienda" data-filtro-valor="${escapeHtml(nombreTienda)}">
                <td class="fw-semibold">${escapeHtml(nombreTienda)}</td>
                <td class="text-end">${formatoNumero(row.total_segmentado)}</td>
                <td class="text-end text-danger fw-semibold">${formatoNumero(row.total_agotado)}</td>
                <td class="text-end">${formatoPorcentaje(row.porcentaje_agotado)}</td>
            </tr>
            `;
        }).join("");
    }

    function pintarDetalle(rows) {
      const tbody = $("#tablaDetalleAgotados");
      if (!tbody) return;

      if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = filaVacia(10);
        return;
      }

      const rowsOrdenadas = ordenarRows(rows, "detalle");

      tbody.innerHTML = rowsOrdenadas.slice(0, 200).map((row) => {
        const agotado = Boolean(row.es_agotado);
        const estadoClass = agotado ? "badge text-bg-danger" : "badge text-bg-success";

        return `
          <tr>
            <td class="fw-semibold">${escapeHtml(row.referencia_sku || "")}</td>
            <td>${escapeHtml(row.descripcion || "")}</td>
            <td>${escapeHtml(row.color || row.codigo_color || "")}</td>
            <td>${escapeHtml(row.talla || "")}</td>
            <td>${escapeHtml(row.desc_dependencia || row.dependencia || "")}</td>
            <td>${escapeHtml(row.ciudad || "")}</td>
            <td>${escapeHtml(row.zona || "")}</td>
            <td class="text-end">${formatoNumero(row.cantidad_segmentada)}</td>
            <td class="text-end">${formatoNumero(row.disponible_calculado)}</td>
            <td><span class="${estadoClass}">${escapeHtml(row.estado_agotado || "")}</span></td>
          </tr>
        `;
      }).join("");
    }

    // =========================================================
    // 5) Estados visuales y helpers
    // =========================================================
    function setLoading(isLoading, message = "Actualizando dashboard...") {
      const btn = $("#btnActualizarAgotados");
      const toast = $("#analiticasLoadingToast");

      if (btn) {
        btn.disabled = isLoading;
        btn.textContent = isLoading ? "Actualizando..." : "Actualizar vista";
      }

      const btnRefresh = $("#btnRefrescarBaseAgotados");
      if (btnRefresh) {
        btnRefresh.disabled = isLoading;
      }

      if (toast) {
        toast.classList.toggle("d-none", !isLoading);
        const textNode = toast.querySelector(".analiticas-loading-text");
        if (textNode) textNode.textContent = message;
      }
    }

    function mostrarError(message) {
      setText("#agotadosMetaTexto", message);

      const linea = $("#chartAgotadosLinea");
      const talla = $("#chartAgotadosTalla");

      if (linea) {
        linea.innerHTML = `<div class="text-danger text-center py-4">${escapeHtml(message)}</div>`;
      }

      if (talla) {
        talla.innerHTML = `<div class="text-danger text-center py-4">${escapeHtml(message)}</div>`;
      }

      const topRefs = $("#tablaReferenciasConAgotados");
      const tiendas = $("#tablaTiendasAgotados");
      const detalle = $("#tablaDetalleAgotados");

      if (topRefs) topRefs.innerHTML = filaVacia(6, message);
      if (tiendas) tiendas.innerHTML = filaVacia(4, message);
      if (detalle) detalle.innerHTML = filaVacia(10, message);
    }
    function norm(v) {
      return (v || "").toString().trim();
    }

    function debounce(fn, delayMs) {
      let t = null;

      return (...args) => {
        clearTimeout(t);
        t = setTimeout(() => fn(...args), delayMs);
      };
    }

    function setDatalistOptions(datalistId, values) {
      const datalist = document.getElementById(datalistId);
      if (!datalist) return;

      const unicos = Array.from(
        new Set((values || []).map(v => norm(v)).filter(Boolean))
      ).sort((a, b) => a.localeCompare(b));

      datalist.innerHTML = unicos
        .map(v => `<option value="${escapeHtml(v)}"></option>`)
        .join("");
    }

    function setText(selector, value) {
      const el = $(selector);
      if (el) el.textContent = value ?? "";
    }

    function formatoNumero(value) {
      const n = Number(value || 0);
      return new Intl.NumberFormat("es-CO").format(n);
    }

    function formatoPorcentaje(value) {
      const n = Number(value || 0);
      return `${n.toFixed(2)}%`;
    }

    function filaVacia(cols, texto = "Sin datos") {
      return `
        <tr>
          <td colspan="${cols}" class="text-center text-muted py-4">
            ${escapeHtml(texto)}
          </td>
        </tr>
      `;
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function actualizarDatalistsAgotados(data) {
      const detalle = Array.isArray(data.detalle) ? data.detalle : [];
      const porLinea = Array.isArray(data.por_linea) ? data.por_linea : [];
      const porTalla = Array.isArray(data.por_talla) ? data.por_talla : [];
      const referencias = Array.isArray(data.referencias_con_agotados)
        ? data.referencias_con_agotados
        : (Array.isArray(data.top_referencias) ? data.top_referencias : []);

      setDatalistOptions(
        "listaAgotadosLineas",
        [
          ...porLinea.map(x => x.linea),
          ...detalle.map(x => x.linea),
        ]
      );

      setDatalistOptions(
        "listaAgotadosReferencias",
        [
          ...referencias.map(x => x.referencia_sku),
          ...detalle.map(x => x.referencia_sku),
        ]
      );

      setDatalistOptions(
        "listaAgotadosReferencias",
        [
          ...referencias.map(x => x.referencia_sku),
          ...detalle.map(x => x.referencia_sku),
        ]
      );

      setDatalistOptions(
        "listaAgotadosClientes",
        detalle.map(x => x.dependencia)
      );

      setDatalistOptions(
        "listaAgotadosTiendas",
        detalle.map(x => x.desc_dependencia || x.dependencia)
      );

      setDatalistOptions(
        "listaAgotadosTipoPortafolio",
        detalle.map(x => x.tipo_portafolio)
      );

      setDatalistOptions(
        "listaAgotadosZonas",
        detalle.map(x => x.zona)
      );

      setDatalistOptions(
        "listaAgotadosClasificaciones",
        detalle.map(x => x.clasificacion)
      );
    }
  });
})();