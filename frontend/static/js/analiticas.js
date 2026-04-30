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

    const $ = (selector) => document.querySelector(selector);

    const state = {
      loading: false,
      lastData: null,
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

      document.addEventListener("click", (event) => {
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

      const refetchDebounced = debounce(() => {
        cargarDashboardAgotados();
      }, 350);

      const filtrosInput = [
        "#filtroLineaAgotados",
        "#filtroCuentoAgotados",
        "#filtroReferenciaAgotados",
        "#filtroClienteAgotados",
        "#filtroTiendaAgotados",
        "#filtroCiudadAgotados",
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

    // =========================================================
    // 3) Filtros
    // =========================================================
    function obtenerFiltros() {
      return {
        linea: valor("#filtroLineaAgotados"),
        cuento: valor("#filtroCuentoAgotados"),
        referencia_sku: valor("#filtroReferenciaAgotados"),

        // Cliente = razón social / dependencia comercial del punto de venta.
        cliente: valor("#filtroClienteAgotados"),

        // Punto de venta = tienda específica.
        dependencia: valor("#filtroTiendaAgotados"),

        ciudad: valor("#filtroCiudadAgotados"),
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
        "#filtroCiudadAgotados",
        "#filtroZonaAgotados",
        "#filtroClasificacionAgotados",
      ].forEach((selector) => {
        const input = $(selector);
        if (input) input.value = "";
      });
    }

    // =========================================================
    // 4) Pintar dashboard
    // =========================================================
    function pintarDashboard(data, meta) {
      const kpis = data.kpis || {};

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
        filtroSelector: "null",
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

      pintarTopReferencias(data.top_referencias || []);
      pintarTiendas(data.por_tienda || []);
      pintarDetalle(data.detalle || []);

      actualizarDatalistsAgotados(data);
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

    function pintarTopReferencias(rows) {
      const tbody = $("#tablaTopReferenciasAgotados");
      if (!tbody) return;

      if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = filaVacia(6);
        return;
      }

      tbody.innerHTML = rows.slice(0, 15).map((row) => `
        <tr class="clickable-row" data-filtro-tipo="referencia" data-filtro-valor="${escapeHtml(row.referencia_sku || "")}">
          <td class="fw-semibold">${escapeHtml(row.referencia_sku || "")}</td>
          <td>${escapeHtml(row.descripcion || "")}</td>
          <td>${escapeHtml(row.linea || "")}</td>
          <td class="text-end">${formatoNumero(row.total_segmentado)}</td>
          <td class="text-end text-danger fw-semibold">${formatoNumero(row.total_agotado)}</td>
          <td class="text-end">${formatoPorcentaje(row.porcentaje_agotado)}</td>
        </tr>
      `).join("");
    }

    function pintarTiendas(rows) {
        const tbody = $("#tablaTiendasAgotados");
        if (!tbody) return;

        if (!Array.isArray(rows) || rows.length === 0) {
            tbody.innerHTML = filaVacia(4);
            return;
        }

        tbody.innerHTML = rows.slice(0, 20).map((row) => {
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
        tbody.innerHTML = filaVacia(9);
        return;
      }

      tbody.innerHTML = rows.slice(0, 200).map((row) => {
        const agotado = Boolean(row.es_agotado);
        const estadoClass = agotado ? "badge text-bg-danger" : "badge text-bg-success";

        return `
          <tr>
            <td class="fw-semibold">${escapeHtml(row.referencia_sku || "")}</td>
            <td>${escapeHtml(row.descripcion || "")}</td>
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
    function setLoading(isLoading) {
      const btn = $("#btnActualizarAgotados");
      const toast = $("#analiticasLoadingToast");

      if (btn) {
        btn.disabled = isLoading;
        btn.textContent = isLoading ? "Actualizando..." : "Actualizar";
      }

      if (toast) {
        toast.classList.toggle("d-none", !isLoading);
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

      const topRefs = $("#tablaTopReferenciasAgotados");
      const tiendas = $("#tablaTiendasAgotados");
      const detalle = $("#tablaDetalleAgotados");

      if (topRefs) topRefs.innerHTML = filaVacia(6, message);
      if (tiendas) tiendas.innerHTML = filaVacia(4, message);
      if (detalle) detalle.innerHTML = filaVacia(9, message);
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
      const topReferencias = Array.isArray(data.top_referencias) ? data.top_referencias : [];

      setDatalistOptions(
        "listaAgotadosLineas",
        [
          ...porLinea.map(x => x.linea),
          ...detalle.map(x => x.linea),
        ]
      );

      setDatalistOptions(
        "listaAgotadosCuentos",
        [
          ...topReferencias.map(x => x.cuento),
          ...detalle.map(x => x.cuento),
        ]
      );

      setDatalistOptions(
        "listaAgotadosReferencias",
        [
          ...topReferencias.map(x => x.referencia_sku),
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
        "listaAgotadosCiudades",
        detalle.map(x => x.ciudad)
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