# Project Journal

## Goal: Update Open Graph (Social Preview) Image
**Timestamp:** 2026-07-25T13:46:44+05:30
**What did we do:** 
We updated the `og:image` and `twitter:image` meta tags in `index.html`. We changed the image paths from the old `theseus-og-picture.png` to the new `assets/og.webp` file.
**Why did we choose to do that:** 
This was done so that whenever the project link is shared on platforms like X (Twitter) or Facebook, the new updated graphic is displayed in the link preview card instead of the old one.

## Goal: SEO Optimization & Domain Correction
**Timestamp:** 2026-07-25T13:48:25+05:30 & 2026-07-25T13:57:25+05:30
**What did we do:** 
We reviewed the existing SEO setup (which already had good basics) and discovered that the absolute links were pointing to the wrong domain (`theseus.sayyedasif.com`). We replaced all instances of that old domain with the actual Cloudflare hosting domain (`theseus.asifdotexe.workers.dev`) across `index.html` (canonical, og:url, schema URL), `robots.txt`, and `sitemap.xml`.
**Why did we choose to do that:** 
Having the wrong domain in canonical tags and sitemaps confuses search engines and can cause them to drop the site from indexing. Additionally, the social preview images wouldn't load if the absolute URL pointed to a domain where the images weren't actually hosted.

## Goal: Debugging 404 Error on Cloudflare Deployment
**Timestamp:** 2026-07-25T14:00:06+05:30 & 2026-07-25T14:32:58+05:30
**What did we do:** 
We investigated why visiting `assets/og.webp` and `/data/` on the live Cloudflare site returned HTTP 404 errors. We identified that `og.webp` was an untracked local file that hadn't been committed to Git or deployed yet. We also verified that visiting the root `/data/` directory fails by design, but accessing a specific file like `/data/processed/react_graph.json` works perfectly.
**Why did we choose to do that:** 
To clear up confusion regarding how web servers behave. Servers (like Cloudflare Workers) disable directory listings for security reasons, so visiting a folder without an `index.html` will always 404. Furthermore, the server cannot serve files that haven't been pushed to the remote build pipeline.

## Goal: Advanced Technical SEO Upgrades
**Timestamp:** 2026-07-25T15:06:13+05:30
**What did we do:** 
- Created a Web App Manifest (`manifest.json`).
- Added `<link rel="manifest">` and Apple Touch icons to `index.html`.
- Added a `<link rel="preload">` tag for `style.css`.
- Added a `<noscript>` tag block summarizing the tool.
- Expanded the existing JSON-LD script from just `WebApplication` to also include a `SoftwareSourceCode` schema.
**Why did we choose to do that:** 
We added the Web App Manifest to make the site mobile-friendly and improve Lighthouse scores. Preloading the CSS improves Core Web Vitals by forcing the browser to load styles faster. The `<noscript>` tag ensures that search engine crawlers that do not execute JavaScript can still read text explaining what the site is. The `SoftwareSourceCode` schema explicitly tells Google that this is an open-source developer tool.

## Goal: Generative Engine Optimization (GEO)
**Timestamp:** 2026-07-25T15:24:01+05:30
**What did we do:** 
- Added a "TL;DR" summary text to the top of the webpage.
- Added a visible "Last Updated" timestamp to the UI next to the author badge.
- Added a Frequently Asked Questions (FAQ) section at the bottom of the page.
- Injected `FAQPage` structured data into the JSON-LD script.
- Added the author's `jobTitle` ("Data Scientist") and a `dateModified` field to the schema.
- Explicitly allowed AI crawlers (`GPTBot`, `Claude-Web`, `PerplexityBot`, `OAI-SearchBot`) in the `robots.txt` file.
**Why did we choose to do that:** 
GEO makes the site highly attractive to AI-powered search engines (like ChatGPT and Perplexity). AI models are trained to extract concise summaries, direct Q&A formats, and authoritative sources. Adding timestamps signals content freshness, the schema provides easily parseable context, and the `robots.txt` changes grant explicit permission for these AI bots to ingest the site's content.

## Goal: Answer Engine Optimization (AEO)
**Timestamp:** 2026-07-25T15:30:50+05:30
**What did we do:** 
- Converted the generic TL;DR text into an explicit AEO Extraction blockquote starting with a direct question: "What is the Ship of Theseus Code Visualizer?".
- Expanded the FAQ section in the HTML from 2 entries to 7 entries, including definitions of 'fossils'.
- Expanded the `FAQPage` structured data in the JSON-LD script to match the 7 entries.
- Ensured all FAQ answers were strictly kept under 50 words and were self-contained.
**Why did we choose to do that:** 
AEO focuses heavily on Voice Search and AI answering engines (like Google's AI Overviews). These engines hunt for "Position Zero" featured snippets. By formatting our main description as a direct Q&A blockquote right after the H1, we feed the extraction engines exactly what they want. Furthermore, AEO best practices dictate having at least 6 concise, self-contained FAQs to signal that the page is a rich source of definitive answers. We specifically added the 'fossil' definition as it clarifies domain-specific terminology that AI models might otherwise misinterpret.

## Impeccable Layout Refactoring

**Goal:** `/impeccable layout faq-section`
**Timestamp:** 2026-07-25T15:43:00+05:30
**What did we do:**
- Removed arbitrary inline styles from the FAQ HTML markup in `index.html`.
- Added semantic `.faq-section`, `.faq-grid`, and `.faq-item` CSS classes to `style.css`.
- Reorganized the flat list of FAQ items into a responsive CSS Grid (`repeat(auto-fit, minmax(280px, 1fr))`).
- Standardized vertical and horizontal rhythm using the project's existing spacing scale (`var(--space-xxl)`, `var(--space-xl)`).
**Why did we choose to do that:**
Following the impeccable layout guidelines, space is treated as a design tool. The previous flat inline-styled list lacked structural grid alignment and rhythm. We moved away from arbitrary padding values toward semantic tokens and used CSS Grid for the 2D layout to provide a responsive, breathing arrangement of the FAQ content. We chose not to place the items in boxes/cards (to avoid 'card monotony') and instead relied on space and typography for hierarchy.

## Impeccable Layout Refactoring for AEO Extraction

**Goal:** `/impeccable layout aeo-extraction`
**Timestamp:** 2026-07-25T15:47:00+05:30
**What did we do:**
- Removed arbitrary inline styles from the `<blockquote class="aeo-extraction">` tag in `index.html`.
- Added the `.aeo-extraction` CSS class definition to `style.css` using the existing design system tokens for spacing (`var(--space-xs)`, `var(--space-md)`) and typography (`var(--mist)`, `var(--ice)`).
- Replaced the hardcoded, bordered, boxed layout with a clean text block that relies solely on spacing, semantic hierarchy, and the `flex-direction: column` structure to separate the bolded question from its answer.
**Why did we choose to do that:**
According to the `impeccable` layout principles, elements should not default to being cards unless there is a strong affordance reason. The AEO block is fundamentally an inline text summary, not an actionable component. By removing the harsh borders and background tint, and by utilizing the established spacing scales and font colors, the extraction snippet integrates seamlessly into the page's rhythm while retaining the semantic importance required for SEO/AEO.

## Impeccable Polish for FAQ and AEO Sections

**Goal:** `/impeccable polish faq-section and aeo-extraction`
**Timestamp:** 2026-07-25T15:55:00+05:30
**What did we do:**
- Corrected the `faq-section` heading class from `.title` (which was incorrectly applying the massive `clamp(2.5rem...)` hero display size) to `.section-title` to align with the rest of the page's section hierarchy.
- Updated `.faq-item h3` to use `var(--font-serif)` (Playfair Display) because these are narrative questions, following the DESIGN.md rule: "serif for philosophy and narrative weight, monospace for data".
- Updated `.aeo-extraction strong` to also use `var(--font-serif)` and increased the font size to `1.1rem` for better alignment with the FAQ structure.
- Polished the margins of the AEO block to perfectly integrate into the hero's flexbox gap rhythm, removing the arbitrary top and bottom margins so it flows naturally beneath the subtitle.
**Why did we choose to do that:**
The `impeccable polish` command demands strict alignment with the existing design system. The FAQ title was a glaring deviation (hero sizing in a regular section), and the typography lacked semantic separation. By assigning the serif font to the questions (narrative) and relying on the parent flex container's gap for spacing, the sections now adhere to the project's precise typography and tonal rules.

## Goal: Comprehensive Architectural and Codebase Audit
**Timestamp:** 2026-07-25T17:30:00+05:30
**What did we do:**
We conducted a highly critical, thorough architectural audit of the entire codebase (frontend and backend) to address scaling bottlenecks, brittleness, and the massive raw JSON dataset issue. We produced an artifact (`architecture_audit.md`) summarizing the findings, which included the recommendation to replace the raw JSON data storage with SQLite or an Append-Only Event Log (JSONL), as well as to modularize the 1,200-line `app.js` "God Object". 
# Project Journal

## Goal: Update Open Graph (Social Preview) Image
**Timestamp:** 2026-07-25T13:46:44+05:30
**What did we do:** 
We updated the `og:image` and `twitter:image` meta tags in `index.html`. We changed the image paths from the old `theseus-og-picture.png` to the new `assets/og.webp` file.
**Why did we choose to do that:** 
This was done so that whenever the project link is shared on platforms like X (Twitter) or Facebook, the new updated graphic is displayed in the link preview card instead of the old one.

## Goal: SEO Optimization & Domain Correction
**Timestamp:** 2026-07-25T13:48:25+05:30 & 2026-07-25T13:57:25+05:30
**What did we do:** 
We reviewed the existing SEO setup (which already had good basics) and discovered that the absolute links were pointing to the wrong domain (`theseus.sayyedasif.com`). We replaced all instances of that old domain with the actual Cloudflare hosting domain (`theseus.asifdotexe.workers.dev`) across `index.html` (canonical, og:url, schema URL), `robots.txt`, and `sitemap.xml`.
**Why did we choose to do that:** 
Having the wrong domain in canonical tags and sitemaps confuses search engines and can cause them to drop the site from indexing. Additionally, the social preview images wouldn't load if the absolute URL pointed to a domain where the images weren't actually hosted.

## Goal: Debugging 404 Error on Cloudflare Deployment
**Timestamp:** 2026-07-25T14:00:06+05:30 & 2026-07-25T14:32:58+05:30
**What did we do:** 
We investigated why visiting `assets/og.webp` and `/data/` on the live Cloudflare site returned HTTP 404 errors. We identified that `og.webp` was an untracked local file that hadn't been committed to Git or deployed yet. We also verified that visiting the root `/data/` directory fails by design, but accessing a specific file like `/data/processed/react_graph.json` works perfectly.
**Why did we choose to do that:** 
To clear up confusion regarding how web servers behave. Servers (like Cloudflare Workers) disable directory listings for security reasons, so visiting a folder without an `index.html` will always 404. Furthermore, the server cannot serve files that haven't been pushed to the remote build pipeline.

## Goal: Advanced Technical SEO Upgrades
**Timestamp:** 2026-07-25T15:06:13+05:30
**What did we do:** 
- Created a Web App Manifest (`manifest.json`).
- Added `<link rel="manifest">` and Apple Touch icons to `index.html`.
- Added a `<link rel="preload">` tag for `style.css`.
- Added a `<noscript>` tag block summarizing the tool.
- Expanded the existing JSON-LD script from just `WebApplication` to also include a `SoftwareSourceCode` schema.
**Why did we choose to do that:** 
We added the Web App Manifest to make the site mobile-friendly and improve Lighthouse scores. Preloading the CSS improves Core Web Vitals by forcing the browser to load styles faster. The `<noscript>` tag ensures that search engine crawlers that do not execute JavaScript can still read text explaining what the site is. The `SoftwareSourceCode` schema explicitly tells Google that this is an open-source developer tool.

## Goal: Generative Engine Optimization (GEO)
**Timestamp:** 2026-07-25T15:24:01+05:30
**What did we do:** 
- Added a "TL;DR" summary text to the top of the webpage.
- Added a visible "Last Updated" timestamp to the UI next to the author badge.
- Added a Frequently Asked Questions (FAQ) section at the bottom of the page.
- Injected `FAQPage` structured data into the JSON-LD script.
- Added the author's `jobTitle` ("Data Scientist") and a `dateModified` field to the schema.
- Explicitly allowed AI crawlers (`GPTBot`, `Claude-Web`, `PerplexityBot`, `OAI-SearchBot`) in the `robots.txt` file.
**Why did we choose to do that:** 
GEO makes the site highly attractive to AI-powered search engines (like ChatGPT and Perplexity). AI models are trained to extract concise summaries, direct Q&A formats, and authoritative sources. Adding timestamps signals content freshness, the schema provides easily parseable context, and the `robots.txt` changes grant explicit permission for these AI bots to ingest the site's content.

## Goal: Answer Engine Optimization (AEO)
**Timestamp:** 2026-07-25T15:30:50+05:30
**What did we do:** 
- Converted the generic TL;DR text into an explicit AEO Extraction blockquote starting with a direct question: "What is the Ship of Theseus Code Visualizer?".
- Expanded the FAQ section in the HTML from 2 entries to 7 entries, including definitions of 'fossils'.
- Expanded the `FAQPage` structured data in the JSON-LD script to match the 7 entries.
- Ensured all FAQ answers were strictly kept under 50 words and were self-contained.
**Why did we choose to do that:** 
AEO focuses heavily on Voice Search and AI answering engines (like Google's AI Overviews). These engines hunt for "Position Zero" featured snippets. By formatting our main description as a direct Q&A blockquote right after the H1, we feed the extraction engines exactly what they want. Furthermore, AEO best practices dictate having at least 6 concise, self-contained FAQs to signal that the page is a rich source of definitive answers. We specifically added the 'fossil' definition as it clarifies domain-specific terminology that AI models might otherwise misinterpret.

## Impeccable Layout Refactoring

**Goal:** `/impeccable layout faq-section`
**Timestamp:** 2026-07-25T15:43:00+05:30
**What did we do:**
- Removed arbitrary inline styles from the FAQ HTML markup in `index.html`.
- Added semantic `.faq-section`, `.faq-grid`, and `.faq-item` CSS classes to `style.css`.
- Reorganized the flat list of FAQ items into a responsive CSS Grid (`repeat(auto-fit, minmax(280px, 1fr))`).
- Standardized vertical and horizontal rhythm using the project's existing spacing scale (`var(--space-xxl)`, `var(--space-xl)`).
**Why did we choose to do that:**
Following the impeccable layout guidelines, space is treated as a design tool. The previous flat inline-styled list lacked structural grid alignment and rhythm. We moved away from arbitrary padding values toward semantic tokens and used CSS Grid for the 2D layout to provide a responsive, breathing arrangement of the FAQ content. We chose not to place the items in boxes/cards (to avoid 'card monotony') and instead relied on space and typography for hierarchy.

## Impeccable Layout Refactoring for AEO Extraction

**Goal:** `/impeccable layout aeo-extraction`
**Timestamp:** 2026-07-25T15:47:00+05:30
**What did we do:**
- Removed arbitrary inline styles from the `<blockquote class="aeo-extraction">` tag in `index.html`.
- Added the `.aeo-extraction` CSS class definition to `style.css` using the existing design system tokens for spacing (`var(--space-xs)`, `var(--space-md)`) and typography (`var(--mist)`, `var(--ice)`).
- Replaced the hardcoded, bordered, boxed layout with a clean text block that relies solely on spacing, semantic hierarchy, and the `flex-direction: column` structure to separate the bolded question from its answer.
**Why did we choose to do that:**
According to the `impeccable` layout principles, elements should not default to being cards unless there is a strong affordance reason. The AEO block is fundamentally an inline text summary, not an actionable component. By removing the harsh borders and background tint, and by utilizing the established spacing scales and font colors, the extraction snippet integrates seamlessly into the page's rhythm while retaining the semantic importance required for SEO/AEO.

## Impeccable Polish for FAQ and AEO Sections

**Goal:** `/impeccable polish faq-section and aeo-extraction`
**Timestamp:** 2026-07-25T15:55:00+05:30
**What did we do:**
- Corrected the `faq-section` heading class from `.title` (which was incorrectly applying the massive `clamp(2.5rem...)` hero display size) to `.section-title` to align with the rest of the page's section hierarchy.
- Updated `.faq-item h3` to use `var(--font-serif)` (Playfair Display) because these are narrative questions, following the DESIGN.md rule: "serif for philosophy and narrative weight, monospace for data".
- Updated `.aeo-extraction strong` to also use `var(--font-serif)` and increased the font size to `1.1rem` for better alignment with the FAQ structure.
- Polished the margins of the AEO block to perfectly integrate into the hero's flexbox gap rhythm, removing the arbitrary top and bottom margins so it flows naturally beneath the subtitle.
**Why did we choose to do that:**
The `impeccable polish` command demands strict alignment with the existing design system. The FAQ title was a glaring deviation (hero sizing in a regular section), and the typography lacked semantic separation. By assigning the serif font to the questions (narrative) and relying on the parent flex container's gap for spacing, the sections now adhere to the project's precise typography and tonal rules.

## Goal: Comprehensive Architectural and Codebase Audit
**Timestamp:** 2026-07-25T17:30:00+05:30
**What did we do:**
We conducted a highly critical, thorough architectural audit of the entire codebase (frontend and backend) to address scaling bottlenecks, brittleness, and the massive raw JSON dataset issue. We produced an artifact (`architecture_audit.md`) summarizing the findings, which included the recommendation to replace the raw JSON data storage with SQLite or an Append-Only Event Log (JSONL), as well as to modularize the 1,200-line `app.js` "God Object". 
**Why did we choose to do that:**
The user specifically requested a review focused on KISS and SOLID principles to simplify the logic and replace the hard-to-review JSON datasets. Storing absolute snapshots of file compositions in JSON for every time period scales terribly (O(Files * Time Periods)) resulting in 21MB files for repos like React, which would break CI limits. Refactoring the data layer and frontend architecture will make the project significantly more predictable, scalable, and maintainable.

## Goal: Establish Baseline Unit Speed & Create Refactoring Plan
**Timestamp:** 2026-07-25T17:50:00+05:30
**What did we do:**
We wrote a benchmarking script (`scripts/benchmark_pipeline.py`) and ran it against the `claude-code` repository to measure the parsing speed of the data pipeline and the exact disk storage bloat. We also created a step-by-step `refactoring_plan.md` outlining exactly how we will decouple the data layer (using serverless append-only JSON) and modularize the frontend `app.js` file, ensuring we have safety nets (tests) before making breaking changes. We did not include a Go/Rust rewrite because the benchmark proved Python is fast enough (1600-3000 lines/sec on incremental blame).
**Why did we choose to do that:**
The user wanted to know the "unit speed" of our operations before we optimize upstream, to ensure our changes are data-driven. The benchmark confirmed that the data storage bloat (44x larger than necessary) is the real bottleneck, not the Python parsing logic. The refactoring plan was created to ensure we tackle these issues without breaking the existing codebase.

## Goal: Execute Refactoring Plan (Data Layer, Frontend Modularization, Security)
**Timestamp:** 2026-07-25T19:10:00+05:30
**What did we do:**
- **Data Layer Optimization**: Decoupled `file_compositions` from the snapshot history into a separate `{repo}_state.json`. Cleaned legacy bloat from history files using `scripts/cleanup_data.py`.
- **Frontend Modularization**: Extracted the monolithic 1200-line `app.js` into strictly scoped ES6 modules (`api.js`, `main.js`, `ui.js`, `chart.js`).
- **Security**: Implemented a Cloudflare Edge Function (`functions/api/request-repo.js`) to handle API requests and secret management securely, removing the `__WEB3FORM_ACCESS_KEY__` secret from the client-side code.
- **Pedagogical Documentation**: Added Sphinx-formatted docstrings and inline comments across all python scripts and javascript modules. We strictly maintained a professional tone while emphasizing the architectural "why" (e.g., explaining why incremental blame state is decoupled, why JS logic is modularized).
**Why did we choose to do that:**
Executing the planned refactoring resolves the core issues of data bloat and frontend brittleness in adherence to KISS and SOLID principles. The explicit, professional docstrings were requested by the user so that any future contributor (or the user themselves) can quickly understand the architectural rationale and prevent regressions.

## Goal: Refine Data Layer Optimization & Revert Frontend Modularization
**Timestamp:** 2026-07-26T14:00:00+05:30
**What did we do:**
- **Frontend Reversion**: Reverted the frontend back to the monolithic `app.js` and `index.html` structure per explicit user preference, completely removing the experimental `js/` folder.
- **JSONL Migration**: Refactored the data layer to use append-only JSON Lines (`_history.jsonl`) instead of giant monolithic JSON files, preventing arbitrary JSON load failures.
- **Fossil Extraction**: Migrated fossil storage into dedicated `{repo}_fossils.json` files, preventing massive history files from being repeatedly read/written.
- **Data Script Robustness**: Rewrote `_data_io.py`, `analyse_repository.py`, and `add_fossils.py` to support the new JSONL schema, use atomic file replacements, and replaced all bare `except` blocks with specific exceptions.
**Why did we choose to do that:**
The user strictly preferred the simpler monolithic frontend architecture (`app.js`), so we reverted the modularization to align with their workflow. The data layer improvements were necessary because storing absolute snapshots of file compositions in a single JSON array was causing massive I/O overhead and pipeline brittleness. Switching to `JSONL` makes incremental appends extremely fast and memory-efficient. Splitting out fossils into their own files prevents unnecessary re-saving of the same historical data. We also addressed opaque error handling by replacing all bare exceptions.

## Goal: High-Performance Rust Rewrite (Data Pipeline)
**Timestamp:** 2026-07-26T15:25:00+05:30
**What did we do:**
- **Halted Python Pipeline:** Killed the Python background pipeline because even with the `JSONL` I/O optimizations, traversing and parsing massive repositories (like `langchain` or `react`) using a `subprocess.run(["git", "blame"])` loop takes 20+ hours. The Python implementation hit a fundamental execution bottleneck.
- **Architectural Shift:** Formulated a plan to rewrite the CPU-bound data pipeline as a standalone Rust CLI tool (`theseus_engine`), following the core advice from `python-to-rust.md`.
- **Ecosystem Swap Strategy:**
  - `argparse` -> `clap` (Command-line arguments).
  - `json` -> `serde` / `serde_json` (Blazing fast, statically-typed JSON serialization).
  - `subprocess.run(["git", "blame"])` -> `git2` (Running git traverse and blame in-memory using C-bindings for libgit2).
  - Single-threaded Python -> `rayon` (Trivially parallelize file-level blames across all CPU cores).
- **Migration Plan:** We will build this in a new `engine/` directory, focusing exclusively on the heavy snapshot analysis first (the most expensive operation).
**Why did we choose to do that:**
Following the `python-to-rust.md` guide, we identified this as the perfect candidate for a Rust migration: it is a pure CPU-bound task, it can be entirely self-contained as a CLI tool (avoiding complex PyO3 interop), and the performance gains from switching from subprocess execution to in-memory native bindings will be astronomical (minutes instead of days). Documenting this here ensures we have a clear, traceable transition path from Python prototyping to Rust production scale.

## Goal: Rust Engine Scaffolding & Developer Experience (DX)
**Timestamp:** 2026-07-26T15:32:00+05:30
**What did we do:**
- **Installed Rust:** Executed `rustup-init` locally to get the `cargo` and `rustc` toolchain online.
- **Engine Scaffolding:** Created a new `engine/` Cargo project and added dependencies for `serde`, `serde_json`, `clap`, `rayon`, `git2`, `log`, and `env_logger`. 
- **Main Skeleton:** Set up the basic `src/main.rs` to use `clap` for parsing the identical CLI arguments expected by `analyse_repository.py` (`--repo-path`, `--output`, `--reprocess`).
- **Bat Scripts for DX:** Since the user isn't deeply familiar with Rust toolchains, we created two helper batch scripts in the project root:
  - `build_engine.bat`: Automates building the Rust binary in `--release` mode.
  - `run_engine.bat`: A wrapper script that lets the user execute the compiled engine exactly as they would a Python script, entirely hiding the underlying `cargo` complexities.
**Why did we choose to do that:**
While we are making a heavy architectural shift to Rust for raw performance, we cannot alienate the end-user or make the project harder to run. By creating simple, double-clickable or easy-to-invoke `.bat` wrappers (`build_engine.bat` / `run_engine.bat`), the user maintains the same ergonomic workflow they had with Python while transparently reaping the massive speed benefits of compiled Rust.

## Goal: Data Pipeline Robustness and Over-engineering Cleanup
**Timestamp:** 2026-07-26T15:10:00+05:30
**What did we do:**
- **Code Review & Cleanup**: Applied a ponytail review to `scripts/cleanup_data.py`, stripping out verbose docstrings, standard library explanations, and shrinking error aggregation logic to minimize the code footprint.
- **Accuracy Improvements**: Updated `get_tracked_files` in `scripts/_utils.py` to accurately filter out empty files, symlinks, and binaries instead of relying solely on `git ls-files`. 
- **Validation Refinements**: Modified `_verify_line_count_guard` in `scripts/analyse_repository.py` to assert exact file counts rather than line counts, completely dropping the arbitrary 1-5% tolerance threshold.
- **Unit Testing**: Replaced the outdated tests in `test_analyse_repository.py` with comprehensive unit tests for `_data_io.py` that create mock file state data structures and strictly assert the output composition exactly matches the input state.
**Why did we choose to do that:**
The user requested a fix for a discrepancy in the `wc -l` guard caused by `git blame` skipping non-text or empty files, which lead to cache invalidation errors. By switching to an exact file-count comparison based on precisely filtered traceable files, we dramatically increase pipeline reliability. Additionally, over-engineered code and verbose docstrings were cleaned up to keep the codebase lean and readable, while unit tests ensure we don't regress the newly decoupled JSON storage mechanics.

## Comprehensive Python to Rust Architecture Mapping

To facilitate learning and guarantee that the reasoning behind the transition to Rust is clear, here is a verbose, 1-to-1 breakdown mapping the old Python data pipeline (`analyse_repository.py`) to the new Rust engine (`engine/src/main.rs`). We outline what Rust offers that Python lacks, and why these features yield a significantly better and faster architecture.

### 1. `get_snapshot_periods(repo_path)`
**Python approach:** 
We previously spawned a subshell using `subprocess.run(["git", "log", "--pretty=format:%H|%cI"])`. Python would capture the massive string output, split it line by line, parse the dates into year-month buckets, and store the final commit hash.
**Rust approach (`get_snapshot_periods`):** 
We use `git2::Repository::open()` and the `revwalk` iterator to walk the commit tree natively in memory. We sort it chronologically and group it by month right on the commit objects using the `chrono` crate.
**New Rust Concepts Introduced:**
- **In-Memory C Bindings (`git2`)**: Instead of shelling out to `git.exe` and incurring OS overhead for every command, `libgit2` links directly to our binary. We read the git internal data structures instantly from memory.
- **`Result<T, E>` & the `?` Operator**: In Python, functions implicitly raise arbitrary exceptions or return None. In Rust, any function that can fail returns a `Result`. The `?` syntax (`let oid = oid_res?;`) safely propagates errors up the chain without needing messy `try/except` blocks.
- **Strongly Typed Iterators**: `revwalk` is a lazy iterator. We don't load the entire git log into RAM; we yield one commit at a time, making it incredibly memory efficient.

### 2. `get_tracked_files(repo_path)`
**Python approach:** 
We ran `subprocess.run(["git", "ls-files"])`, looped through them, and then used `git check-attr -a` or parsed extensions to figure out if files were binary or empty.
**Rust approach (`get_tracked_files`):** 
We grab the snapshot's `Tree` object and call `tree.walk(git2::TreeWalkMode::PreOrder)`. For each file, we check `blob.is_binary()` and `blob.size() > 0`.
**New Rust Concepts Introduced:**
- **Pattern Matching (`if let`)**: Rust forces you to handle states correctly. `if let Ok(blob) = repo.find_blob(...)` cleanly extracts the blob from the Result *only* if it succeeded, elegantly skipping missing files without throwing exceptions.
- **Enums & Sum Types**: The `entry.kind() == Some(ObjectType::Blob)` checks against a strict Enum. You literally cannot compare a Tree to a Blob by accident; the compiler prevents logic bugs.

### 3. `get_changed_files(repo_path, prev_commit, commit)`
**Python approach:** 
We used `subprocess.run(["git", "diff-tree", ...])` and parsed the file paths out of the terminal output to implement incremental cache diffing.
**Rust approach (`get_changed_files`):** 
We use `repo.diff_tree_to_tree()` and iterate over the generated deltas. 
**New Rust Concepts Introduced:**
- **Borrowing and References (`&Tree`, `&mut DiffOptions`)**: Python passes everything by reference natively but relies on the Garbage Collector to clean it up. Rust uses "Lifetimes". `diff_tree_to_tree` borrows the old and new trees via `&Tree`. The memory is strictly managed by the compiler at compile-time, completely eliminating the need for a Garbage Collector, resulting in flat and tiny memory usage charts.

### 4. `_blame_full_snapshot` vs `process_blame`
**Python approach:** 
We used `concurrent.futures.ThreadPoolExecutor` to run `git blame --line-porcelain` in multiple sub-processes. This was our biggest bottleneck. Spawning thousands of `git blame` processes took upwards of 20+ hours for large repositories because of Python's Global Interpreter Lock (GIL) and process spawning overhead.
**Rust approach (`process_blame`):** 
We use the `rayon` crate. We take a vector of file paths, call `files_to_blame.into_par_iter()`, and suddenly the workload is perfectly distributed across every core on your CPU. We open a `Repository` per thread and use `repo.blame_file()`.
**New Rust Concepts Introduced:**
- **Data Parallelism & Zero-Cost Abstractions (`rayon`)**: In Rust, turning a single-threaded loop into a multi-threaded parallel execution is literally as simple as changing `.iter()` to `.par_iter()`.
- **Fearless Concurrency (`Send` and `Sync`)**: Python’s GIL exists because threading is hard to do safely. In Rust, the compiler checks if objects are thread-safe (`Sync` trait). `git2::Repository` is *not* entirely thread-safe, so if we tried to share one instance across all threads, the code *literally would not compile*. Rust forces us to open a `Repository` handle *inside* the thread closure, preventing race conditions entirely.
- **Hunk Walking vs String Parsing**: Instead of parsing massive blocks of text from standard out, `repo.blame_file()` returns memory structures called "Hunks". We simply read `hunk.final_signature().when()` to get the timestamp, instantly bypassing all text parsing overhead.

### 5. Data Structuring and Output
**Python approach:** 
We constructed Python dictionaries (e.g., `defaultdict(int)`) on the fly, relying on duck typing.
**Rust approach:** 
We define a concrete `struct SnapshotData` and use the `serde` crate with `#[derive(Serialize)]`.
**New Rust Concepts Introduced:**
- **Macros (`#[derive(Serialize)]`)**: This attribute tells the Rust compiler to automatically generate lightning-fast JSON serialization code for our struct at compile-time. 
- **Type Guarantee**: `HashMap<String, u32>` strictly enforces that years are strings and line counts are 32-bit unsigned integers (cannot be negative!). If we attempt to insert a float or None, it won't compile. Python would just let it happen and crash at runtime during the JSON dump.
