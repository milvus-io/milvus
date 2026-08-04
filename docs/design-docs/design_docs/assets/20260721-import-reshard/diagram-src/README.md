# Import Re-shard diagram sources

The design document references committed PNG files, so rendering does not require Mermaid, Chrome, Node, or any other diagram tool.

The editable source is `diagrams.html`. It uses only local HTML/CSS and the installed CJK fonts; it has no network dependency. Regenerate every PNG with:

```bash
./render-diagrams.sh
```

The script looks for Google Chrome or Chromium and renders at device scale factor 2. If no browser is installed, or the current sandbox does not permit headless Chrome, the committed PNG files remain the fallback.
