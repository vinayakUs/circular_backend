┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          SCRAPER SYSTEM (COMPLETE)                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                              ┌──────────────────────────────────────────────────────────┐
                              │                   ScraperRegistry                          │ ◄── REGISTRY + SINGLETON
                              ├──────────────────────────────────────────────────────────┤
                              │  - _scrapers: Dict[str, Type[IScraper]]  (private)      │
                              │  - _instances: Dict[str, IScraper]      (private)       │
                              ├──────────────────────────────────────────────────────────┤
                              │  + register(scraper_class: Type[IScraper]) → None       │ ◄── Decorator
                              │  + get(name: str) → IScraper                           │ ◄── FACTORY
                              │  + list_all() → List[IScraper]                         │
                              │  + get_scraper_names() → List[str]                     │
                              └──────────────────────────┬───────────────────────────────┘
                                                         │
                            ┌──────────────────────────────┼──────────────────────────────┐
                            │                              │                              │
                            │         @ScraperRegistry.register                       │
                            │                              │                              │
                            ▼                              ▼                              ▼
               ┌─────────────────────┐        ┌─────────────────────┐        ┌─────────────────────┐
               │      NSEScraper      │        │     SEBIScraper     │        │    BSEScraper       │
               ├─────────────────────┤        ├─────────────────────┤        ├─────────────────────┤
               │ - API_URL            │        │ - HTML_URL          │        │ - (future)          │
               │ - base_url          │        │ - base_url          │        │                     │
               ├─────────────────────┤        ├─────────────────────┤        ├─────────────────────┤
               │ + detect_new()      │        │ + detect_new()      │        │ + detect_new()      │
               │ + get_pdf_url()     │        │ + get_pdf_url()     │        │ + get_pdf_url()     │
               │ + parse_response()  │        │ + parse_html()     │        │ + parse_response()  │
               └─────────┬───────────┘        └─────────┬───────────┘        └─────────┬───────────┘
                         │                            │                            │
                         └────────────────────────────┼────────────────────────────┘
                                                   │ implements
                                                   ▼
                              ┌──────────────────────────────────────────────────────────┐
                              │                     <<interface>> IScraper              │
                              ├──────────────────────────────────────────────────────────┤
                              │  + detect_new(from_date: date, to_date: date)          │
                              │    → List[Circular]                                     │
                              │  + get_pdf_download_url(circular_id: str) → str         │
                              │  + parse_circular_id(raw_id: str) → str               │
                              └──────────────────────────┬───────────────────────────────┘
                                                         │
                                                         │ uses
                                                         ▼
                    ┌───────────────────────────────────────────────────────────────────────────────────────┐
                    │                          ScraperOrchestrator                                       │ ◄── ORCHESTRATOR
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  - db_pool: asyncpg.Pool                                                               │
                    │  - redis_client: redis.Redis                                                         │
                    │  - storage_path: str                                                                 │
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  + run() → None                                                                      │ ◄── Main entry point
                    │  + _scrape_source(source: IScraper) → None                                          │
                    │  + _detect_new_circulars(source: IScraper) → List[Circular]                         │
                    │  + _save_to_database(circular: Circular) → UUID                                     │ ◄── INSERT
                    │  + _download_pdf(pdf_url: str, circular_id: str) → str                            │ ◄── Returns local path
                    │  + _update_file_path(circular_id: UUID, file_path: str) → None                    │ ◄── UPDATE
                    │  + _update_status(circular_id: UUID, status: str) → None                          │
                    │  + _get_last_run_date(source: str) → Optional[date]                                │
                    │  + _set_last_run_date(source: str, date: date) → None                               │
                    └───────────────────────────────────────────────────────────────────────────────────────┘
                                                         │
                                                         │ creates / uses
                                                         ▼
                    ┌───────────────────────────────────────────────────────────────────────────────────────┐
                    │                              Circular (DTO)                                      │ ◄── DATA TRANSFER OBJECT
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  - source: str                           (e.g., "NSE")                             │
                    │  - circular_id: str                      (e.g., "FAOP73629")                       │
                    │  - full_reference: str                   (e.g., "NSE/FAOP/73629")                │
                    │  - department: str                       (e.g., "FAOP")                            │
                    │  - title: str                            (e.g., "Business Continuity...")          │
                    │  - issue_date: date                      (e.g., 2026-04-06)                       │
                    │  - effective_date: Optional[date]        (optional)                               │
                    │  - url: str                              (page URL)                                │
                    │  - pdf_url: str                          (PDF download URL)                       │
                    │  - detected_at: datetime                 (when detected)                          │
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  + to_db_row() → dict                    (for PostgreSQL INSERT)                  │
                    │  + to_json() → str                       (for Redis queue)                        │
                    └───────────────────────────────────────────────────────────────────────────────────────┘
                                                         │
                                                         │ saves to
                                                         ▼
                    ┌───────────────────────────────────────────────────────────────────────────────────────┐
                    │                              PostgreSQL                                           │ ◄── STATE MACHINE
                    │                              circulars (TABLE)                                    │
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  id: UUID (PK)                           │ NOT NULL, DEFAULT gen_random_uuid()      │
                    │  circular_id: VARCHAR(50) (UNIQUE)      │ NOT NULL                                │
                    │  source: VARCHAR(20)                   │ NOT NULL                                │
                    │  department: VARCHAR(20)               │                                          │
                    │  title: TEXT                            │ NOT NULL                                │
                    │  issue_date: DATE                       │ NOT NULL                                │
                    │  effective_date: DATE                  │                                          │
                    │  url: TEXT                              │                                          │
                    │  pdf_url: TEXT                          │                                          │
                    │  file_path: VARCHAR(500)                │ ◄── Local path to downloaded PDF       │
                    │  content_hash: VARCHAR(64)             │ ◄── SHA-256 for deduplication          │
                    │  status: VARCHAR(20)                    │ ◄── DISCOVERED/FETCHED/PARSED/...     │
                    │  error_message: TEXT                   │ ◄── For failed records                 │
                    │  created_at: TIMESTAMPTZ               │ NOT NULL, DEFAULT NOW()                │
                    │  updated_at: TIMESTAMPTZ               │ NOT NULL, DEFAULT NOW()                │
                    ├───────────────────────────────────────────────────────────────────────────────────────┤
                    │  INDEXES:                                                                       │
                    │  - idx_status        ON circulars(status)                                        │
                    │  - idx_source        ON circulars(source)                                       │
                    │  - idx_issue_date    ON circulars(issue_date DESC)                               │
                    └───────────────────────────────────────────────────────────────────────────────────────┘
                                                         │
                                                         │ writes to
                                                         ▼
                    ┌───────────────────────────────────────────────────────────────────────────────────────┐
                    │                              Local File System                                    │
                    │                                                                                    │
                    │  {STORAGE_PATH}/{source}/{year}/{month}/{circular_id}.pdf                         │
                    │                                                                                    │
                    │  Example:                                                                         │
                    │  /data/regulatory_raw/NSE/2026/04/FAOP73629.pdf                                     │
                    │  /data/regulatory_raw/SEBI/2024/11/SEBI_HO_MRD_167.pdf                             │
                    │                                                                                    │
                    └───────────────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          STATUS TRANSITIONS (State Machine)                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
     │ DISCOVERED │────►│  FETCHED  │────►│  PARSED   │────►│ ENRICHED  │────►│ COMPLETE  │
     └────────────┘     └────────────┘     └────────────┘     └────────────┘     └────────────┘
           │                  │                  │                  │                  │
           │                  │                  │                  │                  │
           ▼                  ▼                  ▼                  ▼                  ▼
      (just saved)     (PDF downloaded)   (text extracted)   (NLP done)       (all done)
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           DESIGN PATTERNS USED                                               │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  ┌────────────────────────────────┐   ┌────────────────────────────────┐   ┌────────────────────────────────┐
  │    INTERFACE PATTERN          │   │     REGISTRY PATTERN           │   │    ORCHESTRATOR PATTERN        │
  ├────────────────────────────────┤   ├────────────────────────────────┤   ├────────────────────────────────┤
  │  IScraper                     │   │  ScraperRegistry               │   │  ScraperOrchestrator           │
  │  - defines contract           │   │  - auto-discovers scrapers     │   │  - controls workflow           │
  │  - NSEScraper, SEBIScraper    │   │  - @register decorator         │   │  - coordinates all steps       │
  │    implement it               │   │  - singleton instance          │   │  - manages database           │
  └────────────────────────────────┘   └────────────────────────────────┘   └────────────────────────────────┘
  ┌────────────────────────────────┐   ┌────────────────────────────────┐   ┌────────────────────────────────┐
  │     FACTORY METHOD            │   │        DTO PATTERN             │   │     STATE MACHINE             │
  ├────────────────────────────────┤   ├────────────────────────────────┤   ├────────────────────────────────┤
  │  ScraperRegistry.get()        │   │  Circular                     │   │  circulars.status column      │
  │  - creates scraper by name     │   │  - data transfer between      │   │  - DISCOVERED → FETCHED → ..  │
  │  - returns IScraper           │   │    layers                     │   │  - tracks progress            │
  └────────────────────────────────┘   └────────────────────────────────┘   └────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            HOW TO ADD NEW SOURCE                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  Step 1: Create class implementing IScraper
        @ScraperRegistry.register
        class BSEScraper(IScraper):
            
            def detect_new(self, from_date: date, to_date: date) -> List[Circular]:
                # 1. Call BSE API
                # 2. Parse response
                # 3. Return list of Circular objects
            
            def get_pdf_download_url(self, circular_id: str) -> str:
                # Return BSE PDF download URL
            
            def parse_circular_id(self, raw_id: str) -> str:
                # Normalize BSE circular ID
  Step 2: That's it! No other code changes needed
  Step 3: ScraperOrchestrator.run() automatically picks it up
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            WORKFLOW SEQUENCE                                                    │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  ScraperOrchestrator.run():
  
      1. For each source in ScraperRegistry.list_all():
      
          2. last_date = _get_last_run_date(source.source_name)
          
          3. circulars = source.detect_new(last_date, TODAY)
          
          4. For each circular in circulars:
          
              a. _save_to_database(circular)
                 → INSERT into PostgreSQL (status=DISCOVERED)
                 
              b. file_path = _download_pdf(circular.pdf_url, circular.circular_id)
                 → Save PDF to /data/regulatory_raw/...
                 
              c. _update_file_path(circular.id, file_path)
                 → UPDATE PostgreSQL (file_path=...)
                 
              d. _update_status(circular.id, "FETCHED")
                 → UPDATE PostgreSQL (status=FETCHED)
          
          5. _set_last_run_date(source.source_name, TODAY)
---