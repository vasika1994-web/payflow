"""Swagger UI, который сам подставляет ключ (DOCS_PREAUTHORIZE_API_KEY).

Стандартная страница FastAPI так не умеет. Ключ попадает в HTML, поэтому только для dev/демо.
"""

from __future__ import annotations

import json

from fastapi.responses import HTMLResponse

SECURITY_SCHEME_NAME = "ApiKeyAuth"

SWAGGER_UI_VERSION = "5"

_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{version}/swagger-ui.css">
<title>{title}</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{version}/swagger-ui-bundle.js"></script>
<script>
const preauthorizedKey = {key_json};
const ui = SwaggerUIBundle({{
  url: {openapi_url_json},
  dom_id: "#swagger-ui",
  layout: "BaseLayout",
  deepLinking: true,
  showExtensions: true,
  showCommonExtensions: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
  onComplete: () => {{
    if (preauthorizedKey) {{
      ui.preauthorizeApiKey({scheme_json}, preauthorizedKey);
    }}
  }},
}});
</script>
</body>
</html>
"""


def swagger_ui_html(*, openapi_url: str, title: str, preauthorized_key: str | None) -> HTMLResponse:
    page = _PAGE.format(
        version=SWAGGER_UI_VERSION,
        title=title,
        key_json=json.dumps(preauthorized_key),
        openapi_url_json=json.dumps(openapi_url),
        scheme_json=json.dumps(SECURITY_SCHEME_NAME),
    )
    return HTMLResponse(page)
