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
      // fallback simple por si el runtime no soporta locale
      return `$ ${Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".")}`;
    }
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

    const imagesBaseUrl = norm(config?.imagesBaseUrl);
    const placeholderUrl = norm(config?.placeholderUrl) || "https://via.placeholder.com/120x120?text=IMG";

    const imageResolverUrl = norm(config?.imageResolverUrl);

    const isNew = ref.is_new === true || ref.isNew === true;
    const isSegmented = ref.is_segmented === true || ref.isSegmented === true;

    if (!imageResolverUrl) {
      throw new Error("Falta config.imageResolverUrl para resolver imágenes de referencia.");
    }

    const imgUrl = `${imageResolverUrl}?ref=${encodeURIComponent(referencia)}`;

    const precioUnitario = ref.PrecioUnitario ?? ref.precioUnitario ?? ref.precio_unitario;
    const precioTxt = formatCOP(precioUnitario);

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
          <img
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
              ${isSegmented ? `<span class="badge badge-segmented">Segmentada</span>` : ``}
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

  // Exponemos un único “componente” global, simple y claro
  window.CardReferencia = {
    crearCardHTML
  };
})();
