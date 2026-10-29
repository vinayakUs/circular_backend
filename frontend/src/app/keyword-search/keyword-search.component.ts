import { Component, OnInit, HostListener, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';

import { CircularsApiService, SearchResult, SemanticSearchResponse, Citation } from '../services/circulars-api.service';
import { RecentSearchesService, RecentSearch, SearchMode } from '../services/recent-searches.service';
import { ExchangeSource, exchangeLabel } from '../util/source.util';

type SortOption = 'score' | 'date';
type ModeOption = 'keyword' | 'semantic' | 'phrase' | 'title';

interface SortTab {
  value: SortOption;
  label: string;
}

interface ModeTab {
  value: ModeOption;
  label: string;
}

@Component({
  selector: 'app-keyword-search',
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './keyword-search.component.html',
  styleUrl: './keyword-search.component.css',
  standalone: true
})
export class KeywordSearchComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private sanitizer = inject(DomSanitizer);
  private recentSearches = inject(RecentSearchesService);

  query = '';
  mode: ModeOption = 'keyword';
  source: ExchangeSource = ExchangeSource.ALL;
  sort: SortOption = 'score';
  results: SearchResult[] = [];
  loading = false;
  error = false;
  hasSearched = false;

  // Semantic search state
  semanticResult: SemanticSearchResponse | null = null;
  semanticLoading = false;
  semanticError = false;

  readonly exchanges = Object.keys(ExchangeSource) as (keyof typeof ExchangeSource)[];
  readonly sortOptions: SortTab[] = [
    { value: 'score', label: 'Most Relevant' },
    { value: 'date', label: 'Newest First' },
  ];
  readonly modes: ModeTab[] = [
    { value: 'keyword', label: 'Keyword Search' },
    { value: 'semantic', label: 'Semantic Search' },
    { value: 'phrase', label: 'Phrase Search' },
    { value: 'title', label: 'Title Search' },
  ];

  private searchToken = 0;

  // ── Recent searches dropdown ────────────────────────────────
  showRecentSearches = false;
  recentSearchesList: RecentSearch[] = [];
  private recentSearchBlurTimeout: ReturnType<typeof setTimeout> | null = null;

  // ── Mode menu (replaces native <select>) ────────────────────
  showModeMenu = false;

  ngOnInit(): void {
    this.recentSearchesList = this.recentSearches.getRecent();

    this.route.queryParamMap.subscribe(params => {
      const q = (params.get('q') ?? '').toString();
      const modeParam = (params.get('mode') ?? '').toString();
      this.query = q;
      if (
        modeParam === 'semantic'
        || modeParam === 'keyword'
        || modeParam === 'phrase'
        || modeParam === 'title'
      ) {
        this.mode = modeParam;
      }
      if (q) {
        // A query arriving via the URL counts as a search worth remembering —
        // this is how links from elsewhere in the app land here. Recorded
        // against the mode the link asked for, not the previous one.
        this.recentSearchesList = this.recentSearches.addRecent(q, this.mode);
        this.runSearch();
      }
    });
  }

  /** Close the dropdown on any click outside the search field. */
  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    if (!target) return;
    if (this.showRecentSearches && !target.closest('.search-relative')) {
      this.showRecentSearches = false;
    }
    if (this.showModeMenu && !target.closest('.mode-select-wrapper')) {
      this.showModeMenu = false;
    }
    if (this.activeCitation && !target.closest('.cite-popup') && !target.closest('.cite-chip')) {
      this.closeCitation();
    }
  }

  /** Escape closes whichever menu is open. */
  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.showModeMenu) this.showModeMenu = false;
    if (this.showRecentSearches) {
      this.showRecentSearches = false;
      this.clearBlurTimeout();
    }
    this.closeCitation();
  }

  toggleModeMenu(): void {
    this.showModeMenu = !this.showModeMenu;
  }

  closeModeMenu(): void {
    this.showModeMenu = false;
  }

  onModeOptionClick(value: ModeOption): void {
    this.showModeMenu = false;
    this.onModeChange(value);
  }

  /** Label shown on the mode trigger button — current selection. */
  currentModeLabel(): string {
    return this.modes.find(m => m.value === this.mode)?.label ?? '';
  }

  onSearchFocus(): void {
    this.clearBlurTimeout();
    this.recentSearchesList = this.recentSearches.getRecent();
    this.showRecentSearches = this.recentSearchesList.length > 0;
  }

  onSearchBlur(): void {
    // Deferred so a mousedown on a dropdown row wins the race against blur.
    this.recentSearchBlurTimeout = setTimeout(() => {
      this.showRecentSearches = false;
      this.recentSearchBlurTimeout = null;
    }, 150);
  }

  /** Replay a recent search in the mode it was originally run with. */
  applyRecentSearch(recent: RecentSearch): void {
    this.clearBlurTimeout();
    this.query = recent.q;
    this.mode = recent.mode;
    this.showRecentSearches = false;
    this.onSubmit();
  }

  removeRecentSearch(event: MouseEvent, recent: RecentSearch): void {
    // Stop the row's own mousedown from also running the search.
    event.preventDefault();
    event.stopPropagation();
    this.recentSearchesList = this.recentSearches.removeRecent(recent.q, recent.mode);
    if (this.recentSearchesList.length === 0) {
      this.showRecentSearches = false;
    }
  }

  modeLabel(mode: SearchMode): string {
    const tab = this.modes.find(m => m.value === mode);
    return tab?.label ?? 'Keyword';
  }

  clearRecentSearches(event: MouseEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.recentSearches.clearRecent();
    this.recentSearchesList = [];
    this.showRecentSearches = false;
  }

  private clearBlurTimeout(): void {
    if (this.recentSearchBlurTimeout) {
      clearTimeout(this.recentSearchBlurTimeout);
      this.recentSearchBlurTimeout = null;
    }
  }

  onQueryChange(value: string): void {
    this.query = value;
  }

  onSubmit(): void {
    const q = this.query.trim();
    if (q) {
      this.recentSearchesList = this.recentSearches.addRecent(q, this.mode);
    }
    this.showRecentSearches = false;
    this.runSearch();
  }

  onSourceChange(value: ExchangeSource): void {
    if (this.source === value) return;
    this.source = value;
    this.runSearch();
  }

  getExchangeValue(key: keyof typeof ExchangeSource): ExchangeSource {
    return ExchangeSource[key];
  }

  onSortChange(value: SortOption): void {
    if (this.sort === value) return;
    this.sort = value;
    this.runSearch();
  }

  onModeChange(value: ModeOption): void {
    if (this.mode === value) return;
    this.mode = value;
    // Only flip UI state; the user has to hit Search to run with the new mode.
    this.semanticError = false;
    this.error = false;
  }

  onResultClick(result: SearchResult): void {
    this.router.navigate(['/circular', result.id]);
  }

  exchangeLabel = exchangeLabel;

  badgeClass(source: string): string {
    return (source || '').toLowerCase();
  }

  safePreview(preview: string | undefined): SafeHtml {
    // Backend-built fallback preview (HTML with <mark> tags). Used only when
    // ES highlights are unavailable.
    return this.sanitizer.bypassSecurityTrustHtml(preview ?? '');
  }

  safeHighlight(highlights: Record<string, string[]> | null | undefined): SafeHtml {
    // ES returns the highlighted fragment wrapped in <mark>...</mark> tags
    // (configured via pre_tags/post_tags on the bm25v2 search query).
    // The unified highlighter also escapes other HTML special characters in
    // the source text, so the result is render-ready HTML.
    const fragment = highlights?.['chunk_text']?.[0];
    if (fragment) {
      return this.sanitizer.bypassSecurityTrustHtml(fragment);
    }
    return this.safePreview(undefined);
  }

  safeTitle(title: string, highlights: Record<string, string[]> | null | undefined): SafeHtml {
    // Title-mode searches request highlights on the `title` field with
    // number_of_fragments=0, so the whole title comes back wrapped in
    // <mark>...</mark>. Use it when present, fall back to the raw title.
    const highlighted = highlights?.['title']?.[0];
    if (highlighted) {
      return this.sanitizer.bypassSecurityTrustHtml(highlighted);
    }
    return this.sanitizer.bypassSecurityTrustHtml(title ?? '');
  }

  /**
   * Memoised: a fresh SafeHtml each change-detection cycle would rebuild the
   * citation chips, re-firing `pointerover` and reopening the popup on close.
   */
  get answerHtml(): SafeHtml {
    const source = this.semanticResult;
    if (this.answerHtmlCache && this.answerHtmlCache.source === source) {
      return this.answerHtmlCache.value;
    }
    const value = this.buildAnswerHtml(source);
    this.answerHtmlCache = { source, value };
    return value;
  }

  private answerHtmlCache: { source: SemanticSearchResponse | null; value: SafeHtml } | null = null;

  /**
   * DOMParser is inert and chips use `textContent`, so reference strings from
   * the API can never be interpreted as markup.
   */
  private buildAnswerHtml(result: SemanticSearchResponse | null | undefined): SafeHtml {
    if (!result?.answer) return '';
    const raw = marked.parse(result.answer, { async: false }) as string;
    const citations = result.citations ?? [];
    if (!citations.length) return this.sanitizer.bypassSecurityTrustHtml(raw);

    const byId = new Map(citations.map(c => [c.id, c]));
    const doc = new DOMParser().parseFromString(raw, 'text/html');

    const walker = document.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
    const textNodes: Text[] = [];
    while (walker.nextNode()) {
      textNodes.push(walker.currentNode as Text);
    }

    for (const node of textNodes) {
      const value = node.nodeValue ?? '';
      if (!value.includes('[')) continue;

      const parts = value.split(/(\[\d+\])/g);
      if (parts.length === 1) continue;

      const fragment = doc.createDocumentFragment();
      for (const part of parts) {
        const match = /^\[(\d+)\]$/.exec(part);
        const citation = match ? byId.get(Number(match[1])) : undefined;
        if (!citation) {
          fragment.appendChild(doc.createTextNode(part));
          continue;
        }
        const chip = doc.createElement('button');
        chip.type = 'button';
        chip.className = 'cite-chip';
        chip.dataset['citeId'] = String(citation.id);
        chip.textContent = `[${citation.full_reference || citation.circular_id || citation.id}]`;
        fragment.appendChild(chip);
      }
      node.parentNode?.replaceChild(fragment, node);
    }

    return this.sanitizer.bypassSecurityTrustHtml(doc.body.innerHTML);
  }

  // ── Copy answer ─────────────────────────────────────────────
  answerCopied = false;
  private copiedResetTimer: ReturnType<typeof setTimeout> | null = null;

  async copyAnswer(): Promise<void> {
    const text = this.semanticResult?.answer ?? '';
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      this.markAnswerCopied();
    } catch {
      // navigator.clipboard is undefined outside a secure context (plain
      // http), so fall back to a detached textarea.
      const scratch = document.createElement('textarea');
      scratch.value = text;
      scratch.style.position = 'fixed';
      scratch.style.opacity = '0';
      document.body.appendChild(scratch);
      scratch.select();
      try {
        document.execCommand('copy');
        this.markAnswerCopied();
      } finally {
        scratch.remove();
      }
    }
  }

  private markAnswerCopied(): void {
    this.answerCopied = true;
    if (this.copiedResetTimer) clearTimeout(this.copiedResetTimer);
    this.copiedResetTimer = setTimeout(() => {
      this.copiedResetTimer = null;
      this.answerCopied = false;
    }, 2000);
  }

  // ── Citation popup ───────────────────────────────────────────
  activeCitation: Citation | null = null;
  popupStyle: { top: string; left: string } | null = null;
  private citationHideTimer: ReturnType<typeof setTimeout> | null = null;
  // Click-opened popups persist until a click elsewhere; hover popups follow
  // the pointer.
  private citationPinned = false;

  /** Delegated from the answer container — chips inside `[innerHTML]` cannot carry bindings. */
  onAnswerPointerOver(event: PointerEvent): void {
    if (this.citationPinned) return;
    const chip = this.citeChipFrom(event);
    if (!chip) return;
    this.cancelCitationHide();
    this.showCitation(chip);
  }

  onAnswerPointerOut(event: PointerEvent): void {
    if (this.citationPinned) return;
    // Only chip exits dismiss; crossing the surrounding prose must not.
    if (!this.citeChipFrom(event)) return;
    this.scheduleCitationHide();
  }

  onAnswerClick(event: MouseEvent): void {
    const chip = this.citeChipFrom(event);
    if (!chip) return;
    this.citationPinned = true;
    this.cancelCitationHide();
    this.showCitation(chip);
  }

  onPopupPointerEnter(): void {
    if (this.citationPinned) return;
    this.cancelCitationHide();
  }

  onPopupPointerLeave(): void {
    if (this.citationPinned) return;
    this.scheduleCitationHide();
  }

  private citeChipFrom(event: Event): HTMLElement | null {
    return (event.target as HTMLElement | null)?.closest?.('.cite-chip') ?? null;
  }

  private showCitation(chip: HTMLElement): void {
    const id = Number(chip.dataset['citeId']);
    const citation = (this.semanticResult?.citations ?? []).find(c => c.id === id);
    if (!citation) return;
    this.activeCitation = citation;
    this.popupStyle = this.positionPopup(chip);
  }

  // Deferred so the pointer can cross the chip↔popup gap without the popup
  // dropping out from under it.
  private scheduleCitationHide(): void {
    this.cancelCitationHide();
    this.citationHideTimer = setTimeout(() => {
      this.citationHideTimer = null;
      this.closeCitation();
    }, 150);
  }

  private cancelCitationHide(): void {
    if (this.citationHideTimer) {
      clearTimeout(this.citationHideTimer);
      this.citationHideTimer = null;
    }
  }

  closeCitation(): void {
    this.cancelCitationHide();
    this.citationPinned = false;
    this.activeCitation = null;
    this.popupStyle = null;
  }

  /** Anchors the popup under the chip, flipping above and clamping to the viewport. */
  private positionPopup(chip: HTMLElement): { top: string; left: string } {
    const rect = chip.getBoundingClientRect();
    const gap = 8;
    const width = 340;
    const estimatedHeight = 220;

    let left = rect.left;
    if (left + width > window.innerWidth - 12) {
      left = Math.max(12, window.innerWidth - width - 12);
    }

    const spaceBelow = window.innerHeight - rect.bottom;
    const top = spaceBelow >= estimatedHeight + gap
      ? rect.bottom + gap
      : Math.max(12, rect.top - gap - estimatedHeight);

    return { top: `${top}px`, left: `${left}px` };
  }

  get isKeywordMode(): boolean {
    // Keyword / Exact / Phrase all hit the bm25v2 endpoint today.
    return this.mode !== 'semantic';
  }

  get isSemanticMode(): boolean {
    return this.mode === 'semantic';
  }

  get showKeywordResults(): boolean {
    return this.isKeywordMode && this.results.length > 0;
  }

  get showKeywordEmpty(): boolean {
    return this.isKeywordMode
      && !this.loading
      && this.hasSearched
      && this.results.length === 0
      && !this.error;
  }

  get showKeywordInitial(): boolean {
    return this.isKeywordMode && !this.hasSearched && !this.loading;
  }

  get showKeywordError(): boolean {
    return this.isKeywordMode && this.error;
  }

  get showSemanticAnswer(): boolean {
    return this.isSemanticMode
      && !this.semanticLoading
      && !!this.semanticResult
      && !this.semanticError;
  }

  get showSemanticLoading(): boolean {
    return this.isSemanticMode && this.semanticLoading;
  }

  get showSemanticEmpty(): boolean {
    return this.isSemanticMode
      && !this.semanticLoading
      && this.hasSearched
      && !this.semanticResult
      && !this.semanticError;
  }

  get showSemanticInitial(): boolean {
    return this.isSemanticMode && !this.hasSearched && !this.semanticLoading;
  }

  get showSemanticError(): boolean {
    return this.isSemanticMode && this.semanticError;
  }

  runSearch(): void {
    const q = this.query.trim();
    if (!q) {
      this.results = [];
      this.semanticResult = null;
      this.hasSearched = false;
      this.loading = false;
      this.error = false;
      this.semanticLoading = false;
      this.semanticError = false;
      this.closeCitation();
      return;
    }

    if (this.isSemanticMode) {
      this.runSemanticSearch();
    } else {
      this.runKeywordSearch();
    }
  }

  private runKeywordSearch(): void {
    const token = ++this.searchToken;
    this.loading = true;
    this.error = false;
    this.hasSearched = true;
    this.semanticResult = null;
    this.closeCitation();

    this.apiService.keywordSearchV2({
      query: this.query.trim(),
      source: this.source === 'ALL' ? undefined : this.source,
      sort: this.sort,
      // `title` and `phrase` are wired through to the backend's match_mode;
      // `keyword` is the default (sent as undefined).
      matchMode:
        this.mode === 'title' ? 'title'
        : this.mode === 'phrase' ? 'phrase'
        : undefined,
    }).subscribe({
      next: (response) => {
        if (token !== this.searchToken) return;
        this.results = response.results ?? [];
        this.loading = false;
      },
      error: () => {
        if (token !== this.searchToken) return;
        this.results = [];
        this.loading = false;
        this.error = true;
      }
    });
  }

  private runSemanticSearch(): void {
    const token = ++this.searchToken;
    this.semanticLoading = true;
    this.semanticError = false;
    this.hasSearched = true;
    this.results = [];
    this.closeCitation();

    this.apiService.semanticSearch({
      query: this.query.trim(),
      strategy: 'hybrid',
      source: this.source === 'ALL' ? undefined : this.source,
    }).subscribe({
      next: (response) => {
        if (token !== this.searchToken) return;
        this.semanticResult = response;
        this.semanticLoading = false;
      },
      error: () => {
        if (token !== this.searchToken) return;
        this.semanticResult = null;
        this.semanticLoading = false;
        this.semanticError = true;
      }
    });
  }

}
