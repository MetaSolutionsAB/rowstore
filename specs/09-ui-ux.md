# Web GUI

## Purpose

Documents the built-in web interface for browsing datasets, including display modes, features, and user interaction patterns.

## Scope

Covers the HTML-based dataset browser served at `/dataset/{id}/html`. For the API endpoint definition, see [03-api.md](03-api.md). For the underlying query mechanics, see [05-querying.md](05-querying.md).

## UIX-1 Overview

> **UIX-1.01** RowStore includes a built-in web GUI at `/dataset/{id}/html` powered by Bootstrap Table. It provides a tabular view of dataset contents with filtering and pagination.

## UIX-2 Display Modes

> **UIX-2.01** **Full mode** (default): Renders a complete page with:
> - Navigation bar with branding
> - Header section displaying the dataset API URL and a link to Swagger documentation
> - Data table with column headers and rows
> - Footer
>
> Template files: `webgui_header.html` + `webgui_body_full.html`

> **UIX-2.02** **Embed mode** (`?embed` parameter): Renders a minimal page suitable for iframe embedding:
> - Data table only (no navbar, no header, no footer)
> - Reduced visual chrome for seamless integration into external pages
>
> Template files: `webgui_header.html` + `webgui_body_embed.html`

## UIX-3 Features

> **UIX-3.01** **Server-side pagination**: The GUI uses the `_limit` and `_offset` API parameters to paginate through large datasets. Navigation controls are provided by Bootstrap Table.

> **UIX-3.02** **Per-column filter inputs**: Each column header includes a text input field. Filters are applied on **Enter key** press, sending the filter value as a query parameter to the API.

> **UIX-3.03** **Column visibility toggle**: Users can show or hide individual columns using Bootstrap Table's column visibility controls.

> **UIX-3.04** **Refresh button**: Reloads the current view from the API without changing filters or pagination state.

> **UIX-3.05** **Dynamic column generation**: Column headers are generated dynamically from the dataset's info endpoint (`/dataset/{id}/info`), which provides the list of column names.

## UIX-4 External Links

> **UIX-4.01** The full-mode GUI includes a link to the Swagger documentation for the dataset, pointing to an external Swagger UI instance that loads the dataset's OpenAPI spec from `/dataset/{id}/swagger`.

## UIX-5 Content Negotiation

> **UIX-5.01** A `GET /dataset/{id}` request with `Accept: text/html` receives a **303 See Other** redirect to `/dataset/{id}/html`, seamlessly routing browser users to the web GUI.

## Known Limitations

- No client-side sorting (server-side `_sort` is not implemented)
- No CSV/JSON export button in the GUI (must use the API directly)
- No dark mode or theme customization
- Templates are loaded once at startup (changes require restart)
- Bootstrap Table version is bundled (no CDN option)

## References

- [REST API](03-api.md#api-3-endpoint-catalog) — API endpoints used by the GUI
- [Query Processing](05-querying.md#query-3-special-parameters) — Query parameter handling
- [Glossary](12-glossary.md#glo-1-terms) — Pagination, Query
- Source: `WebGuiResource.java`, `webgui_header.html`, `webgui_body_full.html`, `webgui_body_embed.html`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
