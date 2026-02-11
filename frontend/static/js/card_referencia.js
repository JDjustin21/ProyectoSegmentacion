// frontend/js/card_referencia.js
(function () {
  "use strict";

  function norm(v) {
    return (v || "").toString().trim();
  }

  function getBadge(estadoRaw) {
    const estado = norm(estadoRaw);
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

    return { badgeClass, badgeTextClass, estado };
  }

  // Opcional (para el punto del PrecioUnitario). Si no existe, no muestra nada.
  function formatCOP(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "";
    try {
      return n.toLocaleString("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 });
    } catch {
      return `$ ${Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".")}`;
    }
  }

  // Genera un "badge imagen" (SVG) con el conteo
  function makeBadgeSvg(count) {
    const safe = String(count)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");

    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg" width="64" height="28" viewBox="0 0 64 28">
        <defs>
          <filter id="s" x="-20%" y="-50%" width="140%" height="200%">
            <feDropShadow dx="0" dy="2" stdDeviation="2" flood-opacity="0.30"/>
          </filter>
        </defs>

        <g filter="url(#s)">
          <rect x="1" y="1" rx="14" ry="14" width="62" height="26" fill="rgba(0,0,0,0.70)"/>

          <!-- Icono casa (blanco) -->
          <path d="M14 12.2L20 7.6l6 4.6v8.2c0 .7-.6 1.3-1.3 1.3h-2.9c-.4 0-.8-.4-.8-.8v-4.2h-2.2v4.2c0 .4-.4.8-.8.8h-2.9c-.7 0-1.3-.6-1.3-1.3v-8.2z"
                fill="#fff" opacity="0.95"/>

          <!-- Numero -->
          <text x="44" y="18" text-anchor="middle"
            font-family="Inter, Segoe UI, Arial" font-size="13" font-weight="800"
            fill="#fff">${safe}</text>
        </g>
      </svg>
    `.trim();

    return "data:image/svg+xml;charset=UTF-8," + encodeURIComponent(svg);
  }

  /**
   * Crea el HTML de una card de referencia.
   * - ref: objeto de referencia del dataset
   * - config: { imagesBaseUrl, placeholderUrl }
   */
  function crearCardHTML(ref, config) {
    const referencia = norm(ref.referencia);
    const categoria = norm(ref.categoria);
    const linea = norm(ref.linea);
    const cuento = norm(ref.cuento);
    const descripcion = norm(ref.descripcion);
    const portafolio = norm(ref.tipoPortafolio);
    const color = norm(ref.color);
    const codigoColor = norm(ref.codigoColor);

    const cantidadTallas = Number(ref.cantidadTallas || 0);
    const tallas = norm(ref.tallas);

    const { badgeClass, badgeTextClass, estado } = getBadge(ref.estado);

    const placeholderUrl = norm(config?.placeholderUrl) || "https://via.placeholder.com/120x120?text=IMG";
    const imageResolverUrl = norm(config?.imageResolverUrl);

    const isNew = ref.is_new === true || ref.isNew === true;

    // Fuente de verdad para segmentación en UI: CONTEO > 0
        const tiendasSegRaw =
      ref.tiendas_activas_segmentadas ??
      ref.tiendasActivasSegmentadas ??
      ref.tiendasSegmentadas ??
      ref.tiendas_segmentadas ??
      0;

    const tiendasSeg = Number(tiendasSegRaw);
    const tiendasSegNum = Number.isFinite(tiendasSeg) ? tiendasSeg : 0;
    const showSegBadge = tiendasSegNum > 0;

    if (!imageResolverUrl) {
      throw new Error("Falta config.imageResolverUrl para resolver imágenes de referencia.");
    }

    const imgUrl = `${imageResolverUrl}?ref=${encodeURIComponent(referencia)}`;

    const precioUnitario = ref.PrecioUnitario ?? ref.precioUnitario ?? ref.precio_unitario;
    const precioTxt = formatCOP(precioUnitario);

    // Badge imagen (si hay segmentación real)
    const segBadgeImg = showSegBadge
      ? `<img class="seg-badge" src="${makeBadgeSvg(tiendasSegNum)}" alt="Tiendas segmentadas" title="Tiendas segmentadas: ${tiendasSegNum}">`
      : ``;

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
        data-preciounitario="${norm(precioUnitario)}"
      >
        <div class="product-image">
          ${segBadgeImg}
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
            <div class="product-ref">${referencia}
              ${isNew ? `<span class="ref-new-dot" title="Nueva referencia"></span>` : ``}
            </div>
            <div class="product-badges">
              <span class="badge ${badgeClass} ${badgeTextClass}">${estado || "—"}</span>
            </div>
          </div>

          <div class="product-meta">
            <div><b>Categoría:</b> ${categoria || "—"}</div>
            <div><b>Línea:</b> ${linea || "—"}</div>
            <div><b>Cuento:</b> ${cuento || "—"}</div>

            ${precioTxt ? `
            <div class="product-price-row">
              <span class="product-price-value">${precioTxt}</span>
            </div>
            ` : ``}
          </div>
        </div>
      </div>
    `;
  }

  window.CardReferencia = {
    crearCardHTML
  };
})();
