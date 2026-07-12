// Reserved implementation links and indicator for the guides sidebar.
//
// mdBook's SUMMARY.md cannot express a sidebar entry that points at an external
// URL: a list item's link target must be a chapter file in `src`, and a raw URL
// makes the build fail ("failed to read chapter https://..."). The three
// implementation entries are therefore carried in SUMMARY.md as *draft prefix
// chapters* (bare `[Title]()` links, no chapter files) pinned above
// `[Overview](README.md)` in the same un-numbered prefix block. Production is
// built with mdBook v0.4.40, pinned in `.github/workflows/docs.yml`; that exact
// version renders them as
// <li class="chapter-item expanded affix "><div>...</div></li>.
// The selector below deliberately targets this bare <div> output, which is the
// same version-specific shape used by the ProcessKit-fSharp reference.
// This script upgrades the two external entries to live links and marks the
// local implementation as a non-clickable indicator:
//
//   * "Rust version"   -> a non-clickable indicator for this implementation.
//   * "Python wrapper" -> a live external link to the Python wrapper's docs site.
//   * ".NET version"   -> a live external link to the .NET implementation's site.
//
// Without JS the entries degrade to plain greyed draft items — never a broken or
// misdirected link.
(function () {
  "use strict";

  var ENTRIES = {
    "Rust version": { placeholder: "Current implementation" },
    "Python wrapper": { href: "https://zelanton.github.io/processkit-py/" },
    ".NET version": { href: "https://zelanton.github.io/ProcessKit-fSharp/" }
  };

  function apply() {
    // Production's pinned mdBook v0.4.40 renders draft prefix chapters as:
    // <li class="chapter-item expanded affix "><div>Rust version</div></li>
    // (verified with that exact binary; no leading "N." since prefix chapters
    // are never numbered — the digit-stripping regex below is defensive).
    var drafts = document.querySelectorAll(
      ".sidebar .chapter li.chapter-item > div"
    );

    Array.prototype.forEach.call(drafts, function (draftEntry) {
      var textContent = draftEntry.textContent || "";
      var title = textContent.replace(/^\s*\d+\.\s*/, "").trim();
      var spec = ENTRIES[title];
      if (!spec) {
        return;
      }

      if (spec.href) {
        var link = document.createElement("a");
        link.href = spec.href;
        link.rel = "noopener";
        while (draftEntry.firstChild) {
          link.appendChild(draftEntry.firstChild);
        }
        draftEntry.replaceWith(link);
      } else if (spec.placeholder) {
        draftEntry.classList.add("current-implementation");
        draftEntry.title = spec.placeholder;
        draftEntry.setAttribute("aria-label", title + " — " + spec.placeholder);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", apply);
  } else {
    apply();
  }
})();
