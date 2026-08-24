# Clinch Scraping Project

A hobby project that scrapes UFC/MMA data for clinch-app and stores it in Supabase. Built with Scrapy and deployed serverlessly on AWS Lambda.

## ⚠️ Ethical Disclaimer

**This project is purely a hobby project and has no commercial purpose.**

It is configured to place minimal load on servers by using appropriate delays, limiting concurrent requests, and running infrequently via scheduled triggers.

---

## Architecture Overview

All triggers are fully automated. Scheduled EventBridge cron rules, the Event Date Gatekeeper Lambda invoked by the Supabase events webhook, and the fighter webhook feed into the **AWS Step Functions** master orchestrator, which dispatches to Lambda based on the `task` field.

```
  ┌───────────────────────┐     ┌──────────────────────────────┐     ┌────────────────────────┐
  │   AWS EventBridge     │     │ AWS Lambda: Event Gatekeeper │     │  Supabase DB Webhook   │
  │   (scheduled crons)   │     │ (Supabase DB webhook)        │     │  (new fighter row      │
  │                       │     │ (insert or datetime update)  │     │   inserted)            │
  └──────────┬────────────┘     └─────────────┬────────────────┘     └────────────┬───────────┘
   task: upcoming/event_scan/ranking  task: step_function_loop            task: fighter_scrape
             │                                │                                   │
             └────────────────────────────────┴───────────────────────────────────┘
                                              │
                                              ▼
                               ┌────────────────────────┐
                               │    AWS Step Functions  │
                               │    DetermineTaskType   │
                               │      (Choice State)    │
                               └────┬───────────┬───────┘
                               │    │           │       │
             ┌─────────────────┘    │           │       └─────────────────┐──────────────┐
             │                      │           │                         │              │
             ▼                      ▼           ▼                         ▼              ▼
   ┌──────────────────┐ ┌────────────────┐ ┌───────────────────┐ ┌──────────────┐ ┌───────────────┐
   │RunUpcomingScraper│ │  RunEventScan  │ │WaitUntilEventStart│ │RunFighterScr.│ │RunRankingScr. │
   │                  │ │                │ │ (waits for live)  │ │              │ │               │
   └────────┬─────────┘ └───────┬────────┘ └─────────┬─────────┘ └──────┬───────┘ └───────┬───────┘
            │                   │                    │                  │                 │
            ▼                   ▼                    ▼                  ▼                 ▼
         ✅ Done             ✅ Done         ┌───────────────┐       ✅ Done           ✅ Done
                                             │RunLiveScraper │
                                             │               │◄─────────────┐
                                             └───────┬───────┘              │
                                                     │                      │
                                                     ▼                      │
                                            ┌──────────────────┐            │
                                            │ CheckIfCompleted │            │
                                            └──────┬──────┬────┘            │
                                                   │      │                 │
                                          COMPLETED│      │IN_PROGRESS      │
                                                   │      ▼                 │
                                                   │  WaitForRandomSeconds  │
                                                   │   (90–150s jitter) ────┘
                                                   ▼
                                               ✅ Done
```

---

## Scraping Flow

### 1. Upcoming Mode (`upcoming`)
Fetches the upcoming events from the database (up to 4 events) and refreshes their full card pages to update data.

### 2. Event Scan Mode (`event_scan`)
Once per day, it scrapes the data provider event list and schedules full-page scrapes only for events not already in the database.

### 3. Live Mode (`step_function_loop`)
When a new event is inserted or its `datetime_utc` value changes, Supabase sends a webhook to the Event Date Gatekeeper Lambda. The Lambda starts a Step Functions execution with the `step_function_loop` payload. The state machine waits until the scheduled start time, then invokes Lambda in live mode at intervals determined by jitter until the event status changes to `completed`.

### 4. Fighter Detail Mode (`fighter_scrape`)
Triggered by a **Supabase database webhook** whenever a new fighter row is inserted. Scrapes the fighter's profile page to enrich the record with bio data (nationality, height, weight, etc.).

### 5. Ranking Mode (`ranking`)
Stores up-to-date ranking positions, champion statuses, and rank changes directly in the database.

---

## Anti-Bot Strategy

The project utilizes **Zyte API** as its primary proxy and request unblocking service to easily bypass Cloudflare protections on target sites.

| Technique | How |
|---|---|
| **Zyte API (Primary)** | Managed request unblocking and proxy rotation, configured transparently via `scrapy-zyte-api` middleware. |
| **TLS/HTTP2 Fingerprinting (Legacy/Fallback)** | In older versions, `scrapy-impersonate` was used to mimic real browser handshakes via `curl_cffi`. This remains supported as a local fallback option if needed. |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Scraping Framework | Scrapy |
| Unblocking & Proxies | Zyte API (primary), `scrapy-impersonate` (legacy/fallback) |
| Database | Supabase (PostgreSQL) |
| Deployment | Docker + AWS Lambda |
| Orchestration | AWS Step Functions + EventBridge |
| CI/CD | GitHub Actions |
