/* Accessibility patch for Material for MkDocs.
 *
 * Material renders the header search control as a bare checkbox (`#__search`) with no
 * accessible name, which axe reports as a WCAG 2.2 violation (`label`, critical). The real
 * search text field is labelled already; this gives the toggle one too, and keeps it applied
 * across instant navigation.
 */
(function () {
  "use strict";

  function labelSearchToggle() {
    var toggle = document.getElementById("__search");
    if (toggle && !toggle.getAttribute("aria-label")) {
      toggle.setAttribute("aria-label", "Toggle search");
    }
  }

  labelSearchToggle();
  document.addEventListener("DOMContentLoaded", labelSearchToggle);

  // Material's instant-loading observable re-renders content on navigation.
  if (typeof document$ !== "undefined" && typeof document$.subscribe === "function") {
    document$.subscribe(labelSearchToggle);
  }
})();
