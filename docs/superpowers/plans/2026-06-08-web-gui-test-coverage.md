# Web GUI Test Coverage Improvement Plan

## Goal

Add browser-level and behavior-level coverage for the NiceGUI web interface so regressions in tabs, uploads, drawers, dialogs, and analysis workflows are caught locally before deployment to `ivm-1`.

## Testing Policy

- Run and pass local tests before any VM deployment.
- Deploy to `ivm-1` only after local unit/integration/E2E checks pass.
- Keep TestClient tests for static NiceGUI tree structure and pure helper behavior.
- Add Playwright or NiceGUI user-simulation tests for real browser interactions.

## Priority 1: Source Workflow E2E

- [ ] Paste source -> Analyze -> Generated OMEntity updates without source-tab errors.
- [ ] Upload `.py` file -> upload callback stores source -> Analyze succeeds.
- [ ] Repo source -> Browse DAG -> select DAG -> Analyze succeeds.
- [ ] Empty Paste, empty Upload, no Repo, and no DAG selected show the expected user-facing errors.

## Priority 2: Drawer And Header Behavior

- [ ] Header menu opens/closes Source drawer.
- [ ] Source drawer toggle handle opens/closes Source drawer.
- [ ] Toggle handle icon and position stay synchronized with drawer state.
- [ ] Analyze closes Source drawer after starting analysis.

## Priority 3: DAG Picker Behavior

- [ ] Folder expand/collapse changes visible rows.
- [ ] Search filters DAG files and keeps containing folders visible.
- [ ] Select and double-click choose a DAG and update the DAG Source preview.
- [ ] Refresh reloads the DAG index.

## Priority 4: Settings Dialog Behavior

- [ ] Add repository updates the registered repo list.
- [ ] Remove repository removes it from the UI registry.
- [ ] Git pull selected and Git pull all update status text and refresh repo controls.
- [ ] Settings dialog shows the configured repositories root.

## Priority 5: Results And Actions

- [ ] Copy writes generated OMEntity to clipboard.
- [ ] Save downloads generated OMEntity text.
- [ ] DAG view / Task view toggle re-renders the interactive diagram.
- [ ] Warnings tab shows parser warnings or `No warnings`.

## Priority 6: Auth And Upload Routes

- [ ] Upload endpoint rejects unauthenticated requests.
- [ ] Upload endpoint accepts authenticated/session-cookie requests.
- [ ] WebSocket routes remain protected by Basic Auth/session cookie.

## Priority 7: Visual Smoke Checks

- [ ] Desktop screenshot: header, drawer handle, Source drawer, DAG Source, result tabs.
- [ ] Mobile/narrow screenshot: drawer works as overlay and handle remains visible.
- [ ] DAG picker modal screenshot: compact table, folder icons, search, commit metadata.
