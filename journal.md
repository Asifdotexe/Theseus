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
