---
name: Ship of Theseus
description: Visualizing codebase entropy through the Ship of Theseus paradox
colors:
  abyss: "oklch(2.5% 0.005 260)"
  surface: "oklch(7% 0.008 260)"
  surface-raised: "oklch(10% 0.01 260)"
  seafoam: "oklch(68% 0.14 195)"
  nebula: "oklch(52% 0.22 285)"
  ember: "oklch(72% 0.16 65)"
  ice: "oklch(96% 0.003 255)"
  mist: "oklch(65% 0.015 255)"
  frost: "oklch(45% 0.015 255)"
  border: "oklch(20% 0.01 260 / 0.4)"
  border-hover: "oklch(35% 0.02 260 / 0.5)"
  error-bg: "oklch(20% 0.08 25 / 0.3)"
  error-text: "oklch(70% 0.12 25)"
typography:
  display:
    fontFamily: "'Playfair Display', Georgia, serif"
    fontSize: "clamp(2.5rem, 6vw, 4rem)"
    fontWeight: 700
    lineHeight: 1.1
    letterSpacing: "-0.02em"
  title:
    fontFamily: "'Playfair Display', Georgia, serif"
    fontSize: "clamp(1.5rem, 3vw, 2.2rem)"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "normal"
  body:
    fontFamily: "'JetBrains Mono', 'SF Mono', 'Cascadia Code', monospace"
    fontSize: "0.9rem"
    fontWeight: 400
    lineHeight: 1.6
  label:
    fontFamily: "'JetBrains Mono', 'SF Mono', 'Cascadia Code', monospace"
    fontSize: "0.7rem"
    fontWeight: 500
    lineHeight: 1.4
    letterSpacing: "0.1em"
    textTransform: "uppercase"
  mono:
    fontFamily: "'JetBrains Mono', 'SF Mono', 'Cascadia Code', monospace"
    fontSize: "0.85rem"
    fontWeight: 400
    lineHeight: 1.5
rounded:
  sm: "0.5rem"
  md: "1rem"
  lg: "1.5rem"
  xl: "2rem"
  full: "9999px"
spacing:
  xs: "0.5rem"
  sm: "1rem"
  md: "1.5rem"
  lg: "2rem"
  xl: "3rem"
  xxl: "4rem"
components:
  button-active:
    backgroundColor: "{colors.seafoam}"
    textColor: "{colors.abyss}"
    rounded: "{rounded.sm}"
    padding: "0.4rem 1rem"
  button-default:
    backgroundColor: "transparent"
    textColor: "{colors.mist}"
    rounded: "{rounded.sm}"
    padding: "0.4rem 1rem"
  insight-card:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ice}"
    rounded: "{rounded.xl}"
    padding: "2.5rem"
  glass-panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ice}"
    rounded: "{rounded.xl}"
    padding: "2.5rem"
  badge:
    backgroundColor: "oklch(100% 0 0 / 0.03)"
    textColor: "{colors.mist}"
    rounded: "{rounded.full}"
    padding: "0.4rem 0.9rem"
  selector-pill:
    backgroundColor: "oklch(100% 0 0 / 0.05)"
    rounded: "{rounded.full}"
    padding: "0.5rem"
  fossil-card:
    backgroundColor: "oklch(100% 0 0 / 0.02)"
    textColor: "{colors.ice}"
    rounded: "{rounded.lg}"
    padding: "1.5rem 2rem"
  pill-small:
    backgroundColor: "oklch(100% 0 0 / 0.05)"
    rounded: "{rounded.md}"
    padding: "0.25rem"
  code-block:
    backgroundColor: "oklch(0% 0 0 / 0.4)"
    rounded: "{rounded.md}"
    padding: "0.6rem 1rem"
  tooltip:
    backgroundColor: "oklch(2.5% 0.005 260 / 0.98)"
    rounded: "{rounded.md}"
    padding: "1.75rem"
---

# Design System: Ship of Theseus

## 1. Overview

**Creative North Star: "The Observatory"**

The Observatory is a place of patient observation. You stand at the instrument, peering through the lens at something vast and slow-moving: the evolution of a codebase across years. The interface around you recedes. The data is the view. Every element is designed to keep you in that state of focused attention.

This system is dark not because tools look cool in dark mode, but because the scene demands it: a developer late at night, studying the strata of their repository on a large monitor in a dim room. The dark is planetarium dark, not nightclub dark. Surfaces are distinguished by tonal layering, not blurs or shadows. The palette is restrained by default, with a single seafoam accent providing navigational and interactive signal.

This system explicitly rejects glassmorphism, gradient text, neon-on-black cyberpunk aesthetics, and generic SaaS dashboard conventions.

### Key Characteristics:
- Dark tonal surfaces with subtle light steps for hierarchy
- One accent color (seafoam) for active and interactive elements
- Serif display for narrative weight, monospace body for technical precision
- Flat with tonal layering; no shadows, no blur
- Sharp, responsive state transitions
- Planetarium atmosphere, not dashboard utility

## 2. Colors

The palette is dark with restrained chroma. Neutrals are tinted subtly toward a cool blue hue (chroma 0.005-0.01). The single accent carries the full interactive burden: active buttons, metric highlights, links.

### Primary
- **Seafoam** (oklch(68% 0.14 195)): The single accent. Active button states, metric values, philosophy links, help icon hover, year labels. Used sparingly; its rarity is what gives it weight.

### Secondary
- **Nebula** (oklch(52% 0.22 285)): Secondary signal. The milestone marker star, gradient hints in fossil cards. Rare.

### Tertiary
- **Ember** (oklch(72% 0.16 65)): Tertiary accent. Commit hash badges and fossil year highlights for warmth.

### Neutral
- **Abyss** (oklch(2.5% 0.005 260)): Primary background. The full-canvas ground.
- **Surface** (oklch(7% 0.008 260)): Card and panel background. The raised layer.
- **Surface-raised** (oklch(10% 0.01 260)): Hovered or interactive container state.
- **Ice** (oklch(96% 0.003 255)): Primary text color. High-emphasis content.
- **Mist** (oklch(65% 0.015 255)): Secondary text, labels, muted content.
- **Frost** (oklch(45% 0.015 255)): Tertiary text, placeholder, disabled.
- **Border** (oklch(20% 0.01 260 / 0.4)): Default dividers and container borders.
- **Border-hover** (oklch(35% 0.02 260 / 0.5)): Hovered container borders.

### Named Rules
**The Observatory Dimming Rule.** Chroma decreases as lightness approaches 0 or 100. High-chroma colors at extreme lightness values look garish on a dark canvas. Seafoam at oklch(68% 0.14) is the brightest and most saturated point; nothing exceeds it.

**The One Accent Rule.** Seafoam is the only saturated accent. Purple and orange are used for data differentiation only (chart layers, fossil tags), never for navigation or interactive signal.

## 3. Typography

**Display Font:** Playfair Display (Georgia, serif fallback)
**Body Font:** JetBrains Mono (SF Mono, Cascadia Code fallback)
**Label/Mono Font:** JetBrains Mono (SF Mono, Cascadia Code fallback)

**Character:** The pairing is deliberate: serif for philosophy and narrative weight, monospace for data and engineering precision. The serif is warm and classical; the mono is technical and sharp. They coexist without compromise, each owning its domain.

### Hierarchy
- **Display** (Bold 700, clamp(2.5rem, 6vw, 4rem), 1.1): The main title. Hero only. One line maximum. Solid color, never gradient.
- **Title** (Semibold 600, clamp(1.5rem, 3vw, 2.2rem), 1.2): Section headings, narrative titles, fossil section headings.
- **Body** (Regular 400, 0.9rem, 1.6): All reading text, tooltip content, card descriptions. Capped at 65-75ch.
- **Label** (Medium 500, 0.7rem, 1.4, uppercase, 0.1em letter-spacing): Control labels, card titles, legend items, badge text.
- **Mono** (Regular 400, 0.85rem, 1.5): Code blocks, fossil code content, axis labels, metric values.

## 4. Elevation

This system uses tonal layering, not shadows or blur. Depth is conveyed purely by lightness steps: background (abyss) sits below surface, which sits below surface-raised. There are no box shadows, no backdrop filters. The absence of shadows is deliberate: it keeps the interface quiet and reduces visual noise, letting the data layers in the chart command attention.

### Tonal Layers
- **Depth 0** (abyss): Full-canvas background.
- **Depth 1** (surface): Cards, panels, container backgrounds.
- **Depth 2** (surface-raised): Hovered containers, focused controls, the tooltip backdrop.

### Named Rules
**The Flat-By-Default Rule.** No surface casts a shadow at rest. The only depth cue is tonal. Hovered elements may shift up one tonal layer or translate upward by 2-4px, but they never grow a shadow.

## 5. Components

### Buttons (Repo Selector, Mode/Scale Toggles)
- **Shape:** Sharply curved corners (0.5rem).
- **Default:** Transparent background, mist text color. Blends into the surface.
- **Active:** Seafoam background, abyss text color, no shadow. The color switch is the signal.
- **Hover:** Text transitions to ice (from mist). Active state text uses abyss (white on seafoam).
- **Transition:** 0.2s ease-out on background-color + color.

### Chips (Selector Pills)
- **Not used as standalone chips.** The selector-pill is a group container (full border-radius, dark surface). Buttons sit inside it as described above.

### Cards / Containers (Glass Panels, Insight Cards, Info Cards)
- **Corner Style:** Generous curve (2rem).
- **Background:** Surface tonal layer (oklch(7% 0.008 260)).
- **Shadow Strategy:** None. Flat by default.
- **Border:** 1px solid border color. On hover, border transitions to border-hover.
- **Internal Padding:** 2.5rem (insight card), 1.5rem (info card), 3rem (fossil finder).
- **Hover:** Container translates up 4-8px with same easing, border color shifts to border-hover.

### Inputs / Fields
- None in current scope. The chart interaction is pointer-based, not form-driven.

### Navigation
- Not applicable. Single-page application with no navigation chrome. The repo selector bar serves as navigation.

### Fossil Cards
- **Corner Style:** Curved corners (1.5rem).
- **Background:** Slightly transparent (oklch(100% 0 0 / 0.02)).
- **Border:** 1px solid subtle white (rgba(255,255,255,0.1)). On hover, border shifts toward seafoam.
- **Hover:** 4px upward translate, seafoam glow via box-shadow (no blur backdrop).
- **Padding:** 1.5rem top/bottom, 2rem sides.

### Tooltip (Chart Hover)
- **Background:** Near-opaque abyss (oklch(2.5% 0.005 260 / 0.98)).
- **Border:** 1px solid border color.
- **Corner Style:** 1.25rem.
- **Padding:** 1.75rem.
- **Width:** Minimum 340px.
- **Shadow:** Strong diffuse shadow (box-shadow, not backdrop-filter) for separation from the chart.

### Code Block (Fossil Content)
- **Background:** Near-black (oklch(0% 0 0 / 0.4)).
- **Border:** 1px solid border color.
- **Corner Style:** 0.75rem.
- **Font:** Mono, 0.85rem.
- **Prefix:** Seafoam `>` character (opacity 0.5).

### Badge (Author Badge)
- **Shape:** Full pill (9999px).
- **Background:** Near-white at 3% opacity.
- **Border:** 1px solid border color.
- **Text:** Mist, uppercase, 0.6rem, letter-spacing 0.15em.
- **Hover:** Background intensifies, border lightens, text shifts to ice.

## 6. Do's and Don'ts

### Do:
- **Do** use tonal layering for depth: abyss, surface, surface-raised. No shadows, no blurs.
- **Do** use seafoam as the single interactive accent. Its rarity is the source of its signal.
- **Do** use Playfair Display for narrative, JetBrains Mono for data. Never mix them in the same element.
- **Do** use solid text colors. The main title must never use gradient text.
- **Do** use the flat-by-default rule. Surfaces sit flat until hovered.
- **Do** cap body text at 65-75 characters per line.
- **Do** use ease-out (quart or quint) for transitions. No bounce, no elastic.

### Don't:
- **Don't** use glassmorphism. No backdrop-filter: blur on any surface. The planetarium is not a kaleidoscope.
- **Don't** use gradient text (background-clip: text with gradient). The title is one solid color.
- **Don't** use side-stripe borders. No border-left or border-right over 1px as decorative accent.
- **Don't** replicate the hero-metric template (big number, small label, gradient accent).
- **Don't** create identical card grids with icon + heading + text patterns.
- **Don't** use cyberpunk or hacker aesthetics. No neon-on-black, no glowing borders, no scanlines.
- **Don't** use generic AI slop patterns. If it looks like it was templated, redesign it.
- **Don't** use em dashes. Use commas, colons, or periods.
- **Don't** use `#000` or `#fff`. Tint every neutral toward cool blue (chroma 0.005-0.01).
- **Don't** animate CSS layout properties. Use transform and opacity only.
