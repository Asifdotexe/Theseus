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
