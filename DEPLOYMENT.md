# CTM-PFSD Scanner — Deployment Record

## Status
Local MVP validation complete. Cloud deployment pending billing setup.

## Local URLs
- Backend:  http://localhost:8080
- Frontend: http://localhost:5002/distiller (Firebase emulator)
- Dev server: http://localhost:3000 (Vite dev server)

## Cloud URLs
- Backend (Cloud Run): TBD — update when deployed
- Frontend (Firebase Hosting): https://eleutherios.app/distiller

## To deploy backend (when billing resolved)
Provider options: Railway, Render, Fly.io, Google Cloud Run (Blaze plan)
The Dockerfile is standard and provider-agnostic.
After deployment, update BACKEND_URL in:
  /Volumes/Expansion/Eleutherios/projects/ctm-pfsd-distiller/src/App.jsx  line 8

## To deploy frontend (after backend deployed)
cd /Volumes/Expansion/Eleutherios/projects/ctm-pfsd-distiller
npm run build
cd /Volumes/Expansion/eleutherios-engine3
firebase deploy --only hosting

## Schema version
DistilledGraph schema_version: 1.1 (frozen)
Previous: 1.0 (Phase 1 MVP, frozen)
Next: 2.0 (Phase 3 — actor_graph, breaking change)

## Phase 2 additions (schema v1.1)
Endpoints added:
  GET  /scan-github?url={github_url}
  POST /export

New files: gap_classifier.py, llm_provider_adapter.py,
           github_fetcher.py, policy_exporter.py
